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
	NMap    int
	workers []worker

	finished bool
	lock     sync.Mutex
}

// RPC call
func (c *Coordinator) RegisterWorker(args *WorkerArgs, reply *WorkerReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	workerID := len(c.workers)
	reply.WorkerID = workerID

	currentWorker := worker{}
	currentWorker.State = IDLE
	c.workers = append(c.workers, currentWorker)
	return nil
}

// assign map task to a worker (helper function)
func (c *Coordinator) assignTask(workerID int, task *Task, reply *TaskReply, taskType TaskType) {
	// take the ID from args, mark the task is doing by one worker
	c.workers[workerID].State = IN_PROGRESS
	task.State = IN_PROGRESS
	task.workerID = workerID

	reply.TaskType = taskType
	reply.TaskID = task.ID
	reply.NReduce = c.NReduce

	if taskType == MapTask {
		reply.FileName = task.FileName
	} else {
		reply.NMap = c.NMap
	}
	// TODO: set timer out for the task
}

// RPC call
func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	workerID := args.WorkerID
	completedMapTasks := 0
	for i := 0; i < len(c.MapTasks); i++ {
		task := &c.MapTasks[i]
		if task.State == IDLE {
			c.assignTask(workerID, task, reply, MapTask)
			return nil
		} else if task.State == COMPLETE {
			completedMapTasks++
		}
	}

	if completedMapTasks != c.NMap { // we want to wait till all map task finishes
		reply.TaskType = NoTask
		return nil
	}

	completedReduceTask := 0
	for i := 0; i < len(c.ReduceTasks); i++ {
		task := &c.ReduceTasks[i]
		if task.State == IDLE {
			c.assignTask(workerID, task, reply, ReduceTask)
			return nil
		} else if task.State == COMPLETE {
			completedReduceTask++
		}
	}

	if completedReduceTask != c.NReduce { // we want to wait till all reduce task finishes
		reply.TaskType = NoTask
		return nil
	}

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
	taskType := args.TaskType

	if taskType == MapTask {
		c.MapTasks[taskID].State = COMPLETE
	} else {
		c.ReduceTasks[taskID].State = COMPLETE
	}
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
	c.lock.Lock()
	defer c.lock.Unlock()
	for i := 0; i < c.NMap; i++ {
		if c.MapTasks[i].State != COMPLETE {
			return false
		}
	}
	for i := 0; i < c.NReduce; i++ {
		if c.ReduceTasks[i].State != COMPLETE {
			return false
		}
	}
	return true
}

// initialize coordinator data structure
func (c *Coordinator) init(nMapTask, nReduceTask int) {
	c.MapTasks = make([]Task, nMapTask)
	c.ReduceTasks = make([]Task, nReduceTask)
	c.NReduce = nReduceTask
	c.NMap = nMapTask
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
