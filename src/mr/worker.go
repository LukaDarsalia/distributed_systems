package mr

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// generateUniqueWorkerID generates a unique worker ID using timestamp, PID, and a random number.
func generateUniqueWorkerID(randomPart uint64) string {
	timestamp := time.Now().UnixNano()
	pid := os.Getpid()

	return fmt.Sprintf("worker-%d-%d-%d", timestamp, pid, randomPart)
}

func handlingMap(reply RequestTaskReply, mapf func(string, string) []KeyValue, workerID string) {
	// In FileNames for mapping we will have exactly 1 file
	filename := reply.FileName[0]
	bytes, er := os.ReadFile(filename)
	if er != nil {
		log.Fatalf("Error reading file %s", reply.FileName)
		return
	}

	// Running map function
	keyValues := mapf(filename, string(bytes))

	// Sorting by key
	sort.Sort(ByKey(keyValues))

	tempFiles := make([]*os.File, reply.NReduce)
	finalFileNames := make([]string, reply.NReduce)

	// Creating tmp files
	for i := 0; i < reply.NReduce; i++ {
		tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-%d-%d-", reply.TaskID, i))
		if err != nil {
			log.Fatal("Cannot create temporary file", err)
			return
		}
		tempFiles[i] = tempFile
		finalFileNames[i] = fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
	}

	// Saving kv into tmp files
	for _, kv := range keyValues {
		bucket := ihash(kv.Key) % reply.NReduce
		enc := json.NewEncoder(tempFiles[bucket])
		err := enc.Encode(&kv)
		if err != nil {
			log.Printf("Failed to encode key-value pair %v to file: %v\n", kv, err)
			return
		}
	}

	// Renaming tmp files
	for i := 0; i < reply.NReduce; i++ {
		err := tempFiles[i].Close()
		if err != nil {
			log.Fatal("Cannot close temp file, was already closed", err)
			return
		}

		err = os.Rename(tempFiles[i].Name(), finalFileNames[i])

		if err != nil {
			log.Fatal("Cannot rename temp file", err)
			return
		}
	}

	// Sending filenames to coordinator via RPC
	args := CompleteTaskArgs{
		TaskID:            reply.TaskID,
		WorkerID:          workerID,
		TaskType:          MapTask,
		IntermediateFiles: finalFileNames,
	}
	completeReply := CompleteTaskReply{}
	failedCall := call("Coordinator.ReportTaskCompletion", &args, &completeReply)
	if failedCall == false {
		log.Fatalf("Error calling Coordinator.ReportTaskCompletion: %v", er)
		return
	}
}

func handlingReduce(reply RequestTaskReply, reducef func(string, []string) string, workerID string) {
	// Reading and decoding intermediate data
	intermediate := make(map[string][]string)
	for _, filename := range reply.FileName {
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Failed to open file %s: %v\n", filename, err)
			return
		}

		dec := json.NewDecoder(file)
		var kv KeyValue
		for {
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
		}

		errClose := file.Close()
		if errClose != nil {
			log.Printf("Failed to close file %s: %v\n", filename, errClose)
			return
		}
	}

	// Creates tmp files for outputs
	tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-out-%d-", reply.TaskID))
	if err != nil {
		log.Printf("Failed to create temp file for reduce output: %v\n", err)
		return
	}

	// Calling reduce on each key
	for key, values := range intermediate {
		output := reducef(key, values)
		_, err := fmt.Fprintf(tempFile, "%s %s\n", key, output)
		if err != nil {
			log.Printf("Failed to write to temp file %s: %v\n", tempFile.Name(), err)
			return
		}
	}

	// Closing tmp files
	errClose := tempFile.Close()
	if errClose != nil {
		log.Printf("Failed to close temp file %s: %v\n", tempFile.Name(), errClose)
		return
	}

	// Renaming tmp file
	finalOutputName := fmt.Sprintf("mr-out-%d", reply.TaskID)
	errRename := os.Rename(tempFile.Name(), finalOutputName)
	if errRename != nil {
		log.Printf("Failed to rename temp file %s: %v\n", tempFile.Name(), errRename)
		return
	}

	// Sending complete message to coordinator
	completeArgs := CompleteTaskArgs{
		WorkerID: workerID,
		TaskID:   reply.TaskID,
		TaskType: ReduceTask,
	}
	completeReply := CompleteTaskReply{}
	call("Coordinator.ReportTaskCompletion", &completeArgs, &completeReply)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomPart := r.Uint64()
	worker_id := generateUniqueWorkerID(randomPart)
	for {
		// Step 1: Request tasks every day always.
		args := RequestTaskArgs{WorkerID: worker_id}
		reply := RequestTaskReply{}
		success := call("Coordinator.RequestTask", args, &reply)
		if !success {
			log.Println("Failed to call Coordinator.RequestTask")
			return
		}

		// Step 2: Handle the received task.
		switch reply.TaskType {
		case MapTask:
			// Handle map task
			handlingMap(reply, mapf, worker_id)

		case ReduceTask:
			// Handle reduce task
			handlingReduce(reply, reducef, worker_id)

		case NoneTask:
			// No task available, sleep for a short period before retrying.
			time.Sleep(time.Second / 2)

		case KillSelfTask:
			// Received signal to terminate, break the loop.
			log.Println("Worker received KillSelfTask. Exiting.")
			return
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
