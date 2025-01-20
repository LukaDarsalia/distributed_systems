package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// Enumeration for FOLLOWER, CANDIDATE and LEADER states.
const (
	FOLLOWER  int = 0
	CANDIDATE int = 1
	LEADER    int = 2
)

// DEAD_VALUE rf.dead will be DEAD_VALUE if it is dead
const DEAD_VALUE = 1

// VOTE_NULL is identifier for the node that it has voted for no one yet.
const VOTE_NULL = -1

// Timeouts as constants for better readability of the code!
const (
	LOWER_TIMEOUT     = 150
	UPPER_TIMEOUT     = 300
	HEARTBEAT_TIMEOUT = 50
)

// LogEntry This struct will represent each log entry in log array
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int  // Latest saved term of the state
	votedFor          int  // To whom does one voted for in current term
	currentState      int  // Who is the state currently
	receivedHeartbeat bool // If node received heartbeat between tickers sleep period, its true, else false!

	// Log Replication
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyChan   *chan ApplyMsg

	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.currentState == LEADER
	return term, isleader
}

type InstallSnapshotArgs struct {
	Term              int    // Leader's term
	LeaderId          int    // Leader's ID
	LastIncludedIndex int    // The snapshot replaces all entries up to and including this index
	LastIncludedTerm  int    // Term of lastIncludedIndex
	Data              []byte // Raw bytes of the snapshot
}

type InstallSnapshotReply struct {
	Term int // Current term, for leader to update itself
}

// Implement the InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = VOTE_NULL
		rf.currentState = FOLLOWER
		rf.persist()
	}

	rf.receivedHeartbeat = true

	// If snapshot is old or already installed
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// Discard log entries up to and including lastIncludedIndex
	if args.LastIncludedIndex < rf.log[len(rf.log)-1].Index {
		rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]
	} else {
		// Discard entire log; start fresh
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}

	// Update state machine with the snapshot data, and reset log
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	rf.snapshot = args.Data
	rf.persist()

	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)

	// Apply the snapshot to the state machine
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	DPrintf("Server %d installed snapshot up to index %d", rf.me, args.LastIncludedIndex)

	*rf.applyChan <- applyMsg
}

// Implement the sendInstallSnapshot function.
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	// encodes lastincludeindex and term
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("Error in readPersist: failed to decode")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		if len(rf.log) == 0 {
			rf.log = append(rf.log, LogEntry{Term: 0, Index: 0})
		}
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedIndex = lastIncludedIndex
	}
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.snapshot = snapshot
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		DPrintf("Received old index: %d, rf.lastIncludedIndex=%d", index, rf.lastIncludedIndex)
		return
	}

	cur_log_entry := rf.log[index-rf.lastIncludedIndex]

	// CHECKUP
	if cur_log_entry.Index != index {
		fmt.Fprintf(os.Stderr, "Current log entry index: %d, expected index %d", cur_log_entry.Index, index)
	}
	rf.log = rf.log[index-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = cur_log_entry.Index
	rf.lastIncludedTerm = cur_log_entry.Term
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex
	rf.snapshot = snapshot
	rf.persist()
	DPrintf("Finishes Snapshot()")
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // The term which the candidate has
	CandidateId  int // The unique identifier of the CANDIDATE node
	LastLogIndex int // Index of candidate’s last log entry
	LastLogTerm  int // Term of candidate’s last log entry
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // The term of the FOLLOWER node
	VoteGranted bool // True if the FOLLOWER granted his vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (3A, 3B).
	// If Candidates term is higher
	if args.Term > rf.currentTerm {
		DPrintf("Server %d updating term to %d from Candidate %d's term\n", rf.me, args.Term, args.CandidateId)
		rf.currentState = FOLLOWER
		rf.votedFor = VOTE_NULL
		rf.currentTerm = args.Term
		rf.persist()
	}
	// If Candidates term is lower
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("Server %d (Term %d) received RequestVote from Follower %d (Term %d): VoteGranted=%v\n", args.CandidateId, args.Term, rf.me, args.Term, reply.VoteGranted)
		return
	}

	reply.Term = rf.currentTerm
	// If we haven't voted yet in this term or already voted for this candidate
	if (rf.votedFor == VOTE_NULL || rf.votedFor == args.CandidateId) &&
		rf.isCandidateUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	DPrintf("Server %d (Term %d) received RequestVote from Follower %d (Term %d): VoteGranted=%v\n", args.CandidateId, args.Term, rf.me, args.Term, reply.VoteGranted)
}

func (rf *Raft) isCandidateUpToDate(candidateLastLogIndex int, candidateLastLogTerm int) bool {
	lastLogIndex := len(rf.log) - 1
	var lastLogTerm int
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	} else {
		lastLogTerm = 0
	}

	if candidateLastLogTerm != lastLogTerm {
		return candidateLastLogTerm > lastLogTerm
	} else {
		return candidateLastLogIndex >= rf.log[lastLogIndex].Index
	}
}

// AppendEntriesArgs Currently consists of basic parameters necessary for just heartbeat <3
type AppendEntriesArgs struct {
	Term         int        // Leaders Term
	LeaderId     int        // Leaders ID
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader’s commitIndex
}

// AppendEntriesReply Currently consists of basic parameters necessary for just heartbeat <3
type AppendEntriesReply struct {
	Term          int  // Nodes Term so Leader can update his term
	Success       bool // True if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int
	ConflictIndex int
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %d Received AppendEntries From Leader %d with number of logs=%d, prevLogIndex=%d\n", rf.me, args.LeaderId, len(args.Entries), args.PrevLogIndex)
	DPrintf("Server %d (Term %d) has following logs: %v", rf.me, rf.currentTerm, rf.log)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if rf.currentState != FOLLOWER {
		rf.currentState = FOLLOWER
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = VOTE_NULL
		rf.persist()
	}
	rf.receivedHeartbeat = true

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}

	// Check if log contains an entry at prevLogIndex with term matching prevLogTerm
	if args.PrevLogIndex > rf.log[len(rf.log)-1].Index {
		DPrintf("Server %d (Term %d) has been given invalid index=%d by leader=%d, term=%d, because it has len(log)=%d\n", rf.me, rf.currentTerm, args.PrevLogIndex, args.LeaderId, args.Term, len(rf.log))
		reply.Success = false
		reply.ConflictIndex = rf.log[len(rf.log)-1].Index + 1
		reply.ConflictTerm = -1
		return
	}

	if rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		DPrintf("Server %d (Term %d) log at index=%d term mismatch: expected term %d, got %d\n", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term)
		reply.Success = false
		reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
		// Find the first index of ConflictTerm
		reply.ConflictIndex = rf.firstIndexOfTerm(reply.ConflictTerm)
		return
	}

	// PrevLogIndex and PrevLogTerm match; proceed to append entries
	reply.Success = true

	// Append any new entries not already in the log
	index := args.PrevLogIndex + 1 - rf.lastIncludedIndex
	entryIndex := 0
	for ; entryIndex < len(args.Entries); entryIndex, index = entryIndex+1, index+1 {
		if index < len(rf.log) {
			if rf.log[index].Term != args.Entries[entryIndex].Term {
				// Conflict found, truncate the log
				rf.log = rf.log[:index]
				break
			}
		} else {
			// Reached the end of the follower's log
			break
		}
	}

	// Append any remaining entries from args.Entries
	if entryIndex < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[entryIndex:]...)
		rf.persist()
	}

	// Update commitIndex if needed
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1+rf.lastIncludedIndex)
		DPrintf("Server %d (Term %d) updating commitIndex=%d, len(log)=%d\n", rf.me, rf.currentTerm, rf.commitIndex, len(rf.log))
	}

	DPrintf("Server %d (Term %d) received AppendEntries from Leader %d (Term %d): Success=%v, LogIndex=%d, CommitIndex=%d, LeaderCommit=%d\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, reply.Success, args.PrevLogIndex, rf.commitIndex, args.LeaderCommit)
}

func (rf *Raft) applyUncommited() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied && len(rf.log)+rf.lastIncludedIndex > rf.lastApplied {
			rf.lastApplied++
			if rf.lastApplied <= rf.lastIncludedIndex {
				continue // Skip entries that are in the snapshot
			}
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
				CommandIndex: rf.lastApplied,
			}
			*rf.applyChan <- applyMsg
		}

		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

// sendingAppendEntries This method is used only by leaders when they are sending heartbeats
func (rf *Raft) sendingAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// Attempt to send the RPC call
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//	Attempt to send the RPC call
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		DPrintf("Candidate %d (Term %d) successfully sent RequestVote to Server %d\n", rf.me, args.Term, server)
	} else {
		DPrintf("Candidate %d (Term %d) retrying RequestVote for Server %d\n", rf.me, args.Term, server)
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.currentState == LEADER

	if !isLeader {
		DPrintf("Server %d rejected command %v since it's not the leader.\n", rf.me, command)
		return index, term, isLeader
	}
	index = rf.log[len(rf.log)-1].Index + 1
	newEntry := LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.log = append(rf.log, newEntry)
	rf.persist()
	rf.matchIndex[rf.me] = index

	DPrintf("Leader %d (Term %d) received command %v at log index %d\n", rf.me, term, command, index)

	rf.mu.Unlock()
	rf.sendHeartbeat()
	rf.mu.Lock()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, DEAD_VALUE)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == DEAD_VALUE
}

// ticker is a timeout for starting elections
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Sleeping zzz...
		ms := LOWER_TIMEOUT + (rand.Int63() % (UPPER_TIMEOUT - LOWER_TIMEOUT))
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Decides to start election or not
		rf.mu.Lock()
		if !rf.receivedHeartbeat {
			DPrintf("Server %d (Term %d) timed out waiting for heartbeat. Initiating election.\n", rf.me, rf.currentTerm)
			// No heartbeat?! REVOLUTION (starts elections)!
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.receivedHeartbeat = false
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	// Getting current info
	rf.mu.Lock()
	if rf.currentState != LEADER {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	rf.receivedHeartbeat = true
	rf.mu.Unlock()

	// Iterating over all nodes
	for node := range rf.peers {
		if node == rf.me {
			continue
		}
		// async messages!
		go rf.sendAppendEntriesToFollowers(term, node)
	}
}

func (rf *Raft) sendAppendEntriesToFollowers(term int, node int) {
	for {
		rf.mu.Lock()
		if rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		nextIndex := rf.nextIndex[node]
		if nextIndex < rf.lastIncludedIndex+1 {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.snapshot,
			}

			rf.mu.Unlock()

			reply := &InstallSnapshotReply{}
			if rf.sendInstallSnapshot(node, args, reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = VOTE_NULL
					rf.currentState = FOLLOWER
					rf.persist()
					rf.mu.Unlock()
					return
				}
				// Update nextIndex and matchIndex for the follower
				rf.nextIndex[node] = rf.lastIncludedIndex + 1
				rf.matchIndex[node] = rf.lastIncludedIndex
				rf.mu.Unlock()
				return
			}
			return
		}

		newEntries := make([]LogEntry, 0)
		if nextIndex-rf.lastIncludedIndex < len(rf.log) {
			newEntries = append([]LogEntry{}, rf.log[nextIndex-rf.lastIncludedIndex:]...)
		}
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.log[nextIndex-rf.lastIncludedIndex-1].Term,
			LeaderCommit: rf.commitIndex,
			Entries:      newEntries,
		}

		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		if rf.sendingAppendEntries(node, args, reply) {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = VOTE_NULL
				rf.currentState = FOLLOWER
				rf.persist()
				rf.mu.Unlock()
				return
			}

			if reply.Success {
				if len(args.Entries) > 0 {
					rf.matchIndex[node] = max(rf.matchIndex[node], args.PrevLogIndex+len(args.Entries))
				} else {
					rf.matchIndex[node] = max(rf.matchIndex[node], args.PrevLogIndex)
				}
				rf.nextIndex[node] = max(rf.nextIndex[node], rf.matchIndex[node]+1)
				rf.updateCommitIndex()

				DPrintf("Leader %d (Term %d) received acknowledgment from Follower %d: CommitIndex=%d, nextIndex[%d]=%d\n", rf.me, rf.currentTerm, node, rf.commitIndex, node, rf.nextIndex[node])

				rf.mu.Unlock()
				return
			} else {
				// Use conflict information to adjust nextIndex
				if reply.ConflictTerm != -1 {
					// Leader has entries with ConflictTerm?
					index := -1
					for i := len(rf.log) - 1; i >= 0; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							index = i + rf.lastIncludedIndex
							break
						}
					}
					if index != -1 {
						rf.nextIndex[node] = index + 1
					} else {
						rf.nextIndex[node] = reply.ConflictIndex
					}
				} else {
					// Follower lacks entries; set nextIndex to ConflictIndex
					rf.nextIndex[node] = reply.ConflictIndex
				}
			}
			rf.mu.Unlock()
		} else {
			return
		}
		DPrintf("Retrying AppendEntries to Server %d", node)
		//time.Sleep(HEARTBEAT_TIMEOUT * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex() {
	for N := len(rf.log) - 1; N > rf.commitIndex-rf.lastIncludedIndex; N-- {
		count := 1 // leader itself
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count >= (len(rf.peers)/2)+1 && rf.log[N].Term == rf.currentTerm {
			rf.commitIndex = N + rf.lastIncludedIndex
			//rf.applyCommittedEntries()
			break
		}
	}
}

// heartbeatsTicker Sending heartbeats in HEARTBEAT_TIMEOUT interval
func (rf *Raft) heartbeatsTicker() {
	for rf.killed() == false {
		// Sleeping zzz...
		ms := HEARTBEAT_TIMEOUT
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Sending heartbeat <3
		rf.sendHeartbeat()
	}
}

// startElection Starts election by incrementing term, voting for self and sending sendRequestVote to all node
func (rf *Raft) startElection() {
	rf.mu.Lock()
	// Becomes CANDIDATE
	rf.currentState = CANDIDATE
	// incrementing term
	rf.currentTerm++
	// Votes for self
	var numberOfVotes int32 = 1
	DPrintf("Server %d (Term %d) started election and voted for self.\n", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.persist()
	requestArgs := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogTerm: rf.log[len(rf.log)-1].Term, LastLogIndex: rf.log[len(rf.log)-1].Index}
	rf.mu.Unlock()

	// Describes what is majority
	majority := (len(rf.peers) / 2) + 1

	// Iterates over all nodes
	for node := range rf.peers {
		if node == rf.me {
			continue
		}

		go func(node int) {
			for {
				// Creates arguments struct for RequestVote
				requestReply := RequestVoteReply{}

				// Sends request for a vote
				if rf.sendRequestVote(node, &requestArgs, &requestReply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// If current election is no more erection
					if rf.currentTerm != requestArgs.Term {
						return
					}

					// If Candidates term is outdated
					if rf.currentTerm < requestReply.Term {
						rf.currentTerm = requestReply.Term
						rf.currentState = FOLLOWER
						rf.votedFor = VOTE_NULL
						rf.persist()
						return
					}

					// If voted, increase numberOfVotes
					if requestReply.VoteGranted {
						atomic.AddInt32(&numberOfVotes, 1)
					}
					DPrintf("Server %d received %d/%d votes (majority is %d)\n", rf.me, atomic.LoadInt32(&numberOfVotes), len(rf.peers), majority)

					// If self has majority, it becomes Leader
					if atomic.LoadInt32(&numberOfVotes) >= int32(majority) {
						rf.currentState = LEADER
						rf.nextIndex = make([]int, len(rf.peers))
						for i := range rf.nextIndex {
							rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1 // Initialize each follower's next index to the end of the log
						}
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.matchIndex {
							rf.matchIndex[i] = rf.lastIncludedIndex // Initialize each follower's match index to 0
						}
						DPrintf("Server %d became leader and has %d logs!\n", rf.me, len(rf.log))
						rf.mu.Unlock()
						rf.sendHeartbeat()
						rf.mu.Lock()
						return
					}
					return
				} else {
					time.Sleep(50 * time.Millisecond)
				}
			}
		}(node)
	}
}

func (rf *Raft) firstIndexOfTerm(term int) int {
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Term == term {
			return i
		}
	}
	return len(rf.log) // Should not happen if term exists
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentState = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = VOTE_NULL
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyChan = &applyCh
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: rf.currentTerm, Index: 0}
	rf.lastIncludedTerm = 0
	rf.lastIncludedIndex = 0

	DPrintf("Server %d created with peers %d, initial term %d, state FOLLOWER.\n", rf.me, len(rf.peers), rf.currentTerm)
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start heartbeats goroutine for heartbeats
	go rf.heartbeatsTicker()
	go rf.applyUncommited()
	return rf
}
