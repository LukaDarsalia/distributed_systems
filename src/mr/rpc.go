package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Those are task types
const (
	MapTask      = "MapTask"
	ReduceTask   = "ReduceTask"
	NoneTask     = "NoneTask"
	KillSelfTask = "KillSelfTask"
)

// RequestTaskArgs When requesting task, all we need is workerID.
type RequestTaskArgs struct {
	WorkerID string
}

// RequestTaskReply When assigning task (remember that NoneTask is Task itself) we need those fields.
type RequestTaskReply struct {
	TaskID   int
	TaskType string
	FileName []string
	NReduce  int
}

// CompleteTaskArgs When worker is finished we need those fields.
type CompleteTaskArgs struct {
	WorkerID          string
	TaskID            int
	TaskType          string
	IntermediateFiles []string
}

// CompleteTaskReply For reply we can just send acknowledgment code.
type CompleteTaskReply struct {
	Ack bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
