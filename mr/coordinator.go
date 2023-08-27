package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// store each input files
	MapTasks    []Task
	ReduceTasks []Task

	NReduce int // nReduce is the number of reduce tasks to use.
	workers []worker

	finished bool
	lock     sync.Mutex
}

// RPC call
func (c *Coordinator) RegisterWorker(args *WorkerArgs, reply *WorkerReply) error {
	workerID := len(c.workers)
	reply.WorkerID = workerID

	currentWorker := worker{}
	currentWorker.State = IDLE
	c.workers = append(c.workers, currentWorker)
	return nil
}

// assign map task to a worker (helper function)
func (c *Coordinator) assignMapTask(workerID int, task *Task, reply *TaskReply) {
	// take the ID from args, mark the task is doing by one worker
	c.workers[workerID].State = IN_PROGRESS
	task.State = IN_PROGRESS
	task.workerID = workerID

	reply.TaskType = MapTask
	reply.TaskID = task.ID
	reply.FileName = task.FileName
	reply.NReduce = c.NReduce

	// TODO: set timer out for the task
}

// RPC call
func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	for i := 0; i < len(c.MapTasks); i++ {
		task := &c.MapTasks[i]
		if task.State == IDLE {
			// need to process
			workerID := args.WorkerID
			c.assignMapTask(workerID, task, reply)
			return nil
		}
	}

	// for _, task := range c.ReduceTasks {

	// }

	// arrive here, we should terminate the worker
	reply.TaskType = ExitTask
	return nil
}

// RPC call
func (c *Coordinator) CompleteTask(args *TaskArgs, reply *TaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	taskID := args.TaskID
	workerID := args.WorkerID

	c.MapTasks[taskID].State = COMPLETE
	c.workers[workerID].State = IDLE
	reply.Success = true
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
func (c *Coordinator) Done() bool {
	return c.finished
}

// initialize coordinator data structure
func (c *Coordinator) init(nMapTask, nReduceTask int) {
	c.MapTasks = make([]Task, nMapTask)
	c.ReduceTasks = make([]Task, nReduceTask)
	c.NReduce = nReduceTask
	c.finished = false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// initialize the data Structure
	c.init(len(files), nReduce)

	for i, fileName := range files {
		mapTask := Task{ID: i, State: IDLE, FileName: fileName, Type: MapTask}
		c.MapTasks[i] = mapTask
	}

	for i := 0; i < nReduce; i++ {
		reduceTask := Task{ID: i, State: IDLE, Type: ReduceTask}
		c.ReduceTasks[i] = reduceTask
	}

	c.server()
	return &c
}
