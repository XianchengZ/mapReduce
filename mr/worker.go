package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sort"
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
			log.Printf("Worker %d start map task %d, file: %v\n", worker.ID, reply.TaskID, reply.FileName)
			worker.doMapTask(mapf, reply.FileName, reply.NReduce, reply.TaskID)

		} else if reply.TaskType == ReduceTask {
			log.Printf("Worker %d start reduce task %d\n", worker.ID, reply.TaskID)
			worker.doReduceTask(reducef, reply.NMap, reply.TaskID)
		} else if reply.TaskType == ExitTask {
			log.Printf("Worker %d terminated by coordinator\n", worker.ID)
			os.Exit(0)
		}
	}
}

func (worker *worker) doMapTask(
	mapf func(string, string) []KeyValue,
	fileName string,
	nReduce int,
	taskID int,
) {
	log.Printf("	map task performed by worker: %d, file name: %v\n", worker.ID, fileName)

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
	taskArgs.TaskType = MapTask

	taskReply := TaskReply{}

	call("Coordinator.CompleteTask", &taskArgs, &taskReply)
	if !taskReply.Success {
		log.Fatalf("Something wrong with updating %v task %d in coordinator\n", MapTask, taskID)
	}
}

func (worker *worker) doReduceTask(
	reducef func(string, []string) string,
	nMap int,
	reducePartitionNumber int,
) {
	// iterate through all task ID by range of nmap, add intermediate kvp together
	//	that belongs to the same reduce tasks
	var kvpArray []KeyValue

	for i := 0; i < nMap; i++ {
		// read intermediate file contents, merge together in RAM
		currentFileName := fmt.Sprintf("mr-%v-%v", i, reducePartitionNumber)
		currentKVPArray := decodeJSONFromFile(currentFileName)
		kvpArray = append(kvpArray, currentKVPArray...)
	}

	// sort kvpArray to group same intermediate keys together
	sort.SliceStable(kvpArray, func(i, j int) bool {
		return kvpArray[i].Key < kvpArray[j].Key
	})

	// create output file
	outputFileName := fmt.Sprintf("mr-out-%d", reducePartitionNumber)
	outputFile, err := os.Create(outputFileName + ".tmp")
	if err != nil {
		log.Fatalf("Could not create file %v\n", outputFileName+".tmp")
	}
	// loop through kvp, for append values of the same intermediate key together
	// pass to user defined reduce function
	i := 0
	for i < len(kvpArray) {
		var currentValues []string
		currentKey := kvpArray[i].Key
		currentValues = append(currentValues, kvpArray[i].Value)

		j := i + 1
		for j < len(kvpArray) && kvpArray[j].Key == currentKey {
			currentValues = append(currentValues, kvpArray[j].Value)
			j++
		}

		// given the key and list of values, pass to user defined reduce func
		output := reducef(currentKey, currentValues)
		fmt.Fprintf(outputFile, "%v %v\n", currentKey, output)

		// ready for next loop
		if j == len(kvpArray) {
			break
		}
		i = j
	}
	outputFile.Close()
	os.Rename(outputFileName+".tmp", outputFileName)

	// update that the reduce task is done
	taskArgs := TaskArgs{}
	taskArgs.WorkerID = worker.ID
	taskArgs.TaskID = reducePartitionNumber
	taskArgs.TaskType = ReduceTask

	taskReply := TaskReply{}

	call("Coordinator.CompleteTask", &taskArgs, &taskReply)
	if !taskReply.Success {
		log.Fatalf("Something wrong with updating %v task %d in coordinator\n", ReduceTask, reducePartitionNumber)
	} else {
		log.Printf("	Reduce Task performed by worker: %d\n", worker.ID)
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
