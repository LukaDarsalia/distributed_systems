package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// Timeout
const Timeout = time.Second * 10

// Task State
const (
	Idle       = "idle"        // When Task isn't started, it has Idle State.
	InProgress = "in-progress" // When Task is in progress, it has InProgress State.
	Completed  = "completed"   // When Task is completed, it has Completed State.
)

// Task Struct
type Task struct {
	State     string    // Idle, InProgress or Completed.
	Worker    string    // WorkerID which is empty if is Idle.
	StartTime time.Time // Timestamp when the task was assigned to a worker.
}

type Coordinator struct {
	mu            sync.Mutex       // For locking each method in Coordinator which is called in non-synchronous manner.
	mapTasks      map[int]*Task    // Map Tasks map where we have id and corresponding Task Struct.
	reduceTasks   map[int]*Task    // Reduce Tasks map where we have id and corresponding Task Struct.
	intermediate  map[int][]string // Intermediate files saved like reducer_worker_id: mapped_filenames.
	nReduce       int              // Number of reducers that we have.
	mapFiles      []string         // Files that need to be mapped.
	completedMaps int              // Number of completed Map functions.
}

// NewTask Initializing
func NewTask() *Task {
	return &Task{
		State:  Idle,
		Worker: "",
	}
}

// RequestTask an RPC call from worker to ask new tasks. Task can be MapTask, ReduceTask, NoneTask or KillSelfTask
func (m *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Checking if all processes are done. If yes, we will tell workers to kill themselves!
	isDone := true

	// Check if all map tasks are completed
	if m.completedMaps != len(m.mapFiles) {
		isDone = false
	}

	// Check if all reduce tasks are completed
	for _, task := range m.reduceTasks {
		if task.State != Completed {
			isDone = false
		}
	}

	// If is done, kill
	if isDone {
		reply.TaskType = KillSelfTask
		reply.TaskID = 0
		reply.FileName = []string{"0"}
		reply.NReduce = m.nReduce
		return nil
	}

	if m.completedMaps != len(m.mapFiles) {
		for id, task := range m.mapTasks {
			if task.State == Idle {
				task.State = InProgress
				task.Worker = args.WorkerID
				task.StartTime = time.Now()
				reply.TaskType = MapTask
				reply.TaskID = id
				reply.FileName = []string{m.mapFiles[id]}
				reply.NReduce = m.nReduce
				return nil
			}
		}
	} else {
		for id, task := range m.reduceTasks {
			if task.State == Idle {
				task.State = InProgress
				task.Worker = args.WorkerID
				task.StartTime = time.Now()
				reply.TaskType = ReduceTask
				reply.TaskID = id
				reply.FileName = m.intermediate[id]
				reply.NReduce = m.nReduce
				return nil
			}
		}
	}

	// If no tasks are available, indicate no task to reply
	reply.TaskType = NoneTask
	return nil
}

// ReportTaskCompletion is an RPC call. When worker finishes work, it sends report. If task was KillSelfTask or
// NoneTask it won't send anything.
func (m *Coordinator) ReportTaskCompletion(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.TaskType == MapTask {
		task := m.mapTasks[args.TaskID]
		if task.State == InProgress && task.Worker == args.WorkerID {
			task.State = Completed
			// Update the intermediate files map
			for _, intermediateFile := range args.IntermediateFiles {
				// Extract the reduce task ID (Y) from the intermediate file name mr-X-Y
				reduceTaskID := extractReduceTaskID(intermediateFile)

				if reduceTaskID == -1 {
					log.Printf("Ignore task completion for intermediate file %s", intermediateFile)
					continue
				}

				// Append the intermediate file to the correct reduce task list
				m.intermediate[reduceTaskID] = append(m.intermediate[reduceTaskID], intermediateFile)
			}
			m.completedMaps++
		}
	} else if args.TaskType == ReduceTask {
		task := m.reduceTasks[args.TaskID]
		if task.State == InProgress && task.Worker == args.WorkerID {
			task.State = Completed
		}
	}

	reply.Ack = true
	return nil
}

func extractReduceTaskID(file string) int {
	var mapTaskID, reduceTaskID int
	_, err := fmt.Sscanf(file, "mr-%d-%d", &mapTaskID, &reduceTaskID)
	if err != nil {
		return -1
	}
	return reduceTaskID
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Coordinator) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if all map tasks are completed
	if m.completedMaps != len(m.mapFiles) {
		return false
	}

	// Check if all reduce tasks are completed
	for _, task := range m.reduceTasks {
		if task.State != Completed {
			return false
		}
	}

	log.Println("All tasks are completed. Coordinator is done. Now sleeping for 10 seconds.")
	time.Sleep(Timeout)

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{
		mapTasks:     make(map[int]*Task),
		reduceTasks:  make(map[int]*Task),
		intermediate: make(map[int][]string),
		nReduce:      nReduce,
		mapFiles:     files,
	}

	// Initialize map tasks
	for i := range files {
		m.mapTasks[i] = NewTask()
	}

	// Initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = NewTask()
	}

	m.server()

	go func() {
		for {
			time.Sleep(Timeout / 2) // Check every half timeout duration.
			m.mu.Lock()
			for id, task := range m.mapTasks {
				if task.State == InProgress && time.Since(task.StartTime) > Timeout {
					task.State = Idle // Mark the task as idle again.
					task.Worker = ""  // Remove the worker assignment.
					log.Printf("Map task %d has timed out and is now idle again.\n", id)
				}
			}
			for id, task := range m.reduceTasks {
				if task.State == InProgress && time.Since(task.StartTime) > Timeout {
					task.State = Idle
					task.Worker = ""
					log.Printf("Reduce task %d has timed out and is now idle again.\n", id)
				}
			}
			m.mu.Unlock()
		}
	}()
	return &m
}
