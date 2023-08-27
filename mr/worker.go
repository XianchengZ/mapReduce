package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
)

type worker struct {
	ID    int
	State State
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// input two user-defined functions, map and reduce

	// register worker at coordinator
	worker := worker{}
	workerArgs := WorkerArgs{}
	workerReply := WorkerReply{}

	isSuccess := call("Coordinator.RegisterWorker", &workerArgs, &workerReply)
	if !isSuccess {
		os.Exit(1)
	}
	worker.ID = workerReply.WorkerID

	for {
		// asking for tasks from coordinator
		args := TaskArgs{}
		reply := TaskReply{}

		args.WorkerID = worker.ID

		isSuccess = call("Coordinator.RequestTask", &args, &reply)
		if !isSuccess {
			os.Exit(1)
		}

		// then read that file and call the application Map function
		if reply.TaskType == MapTask {
			worker.doMapTask(mapf, reply.FileName, reply.NReduce, reply.TaskID)
			// call map task

		} else if reply.TaskType == ReduceTask {
			// call reduce task
		} else {
			log.Printf("Worker %d terminated by coordinator\n", worker.ID)
			os.Exit(0)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func (worker *worker) doMapTask(
	mapf func(string, string) []KeyValue,
	fileName string,
	nReduce int,
	taskID int,
) {
	log.Printf("map task performed by worker: %d, file name: %v\n", worker.ID, fileName)

	// read from the file
	contents := readFromFile(fileName)

	// intermediateKVP will be an array of KVP . e.g. []KeyValue
	intermediateKVP := mapf(fileName, string(contents))

	// partition into n buckets
	partitionedKVP := partition(intermediateKVP, nReduce)

	// write intermediate results out according to intermediate keys
	writeToIntermediateFiles(partitionedKVP, taskID)

	// update coordinator's data structure for the task and the worker status
	taskArgs := TaskArgs{}
	taskArgs.WorkerID = worker.ID
	taskArgs.TaskID = taskID

	taskReply := TaskReply{}

	call("Coordinator.CompleteTask", &taskArgs, &taskReply)
	if !taskReply.Success {
		log.Fatalf("Something wrong with updating task %d in coordinator\n", taskID)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
