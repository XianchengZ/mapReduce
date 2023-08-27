package mr

// enum of different task type
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	ExitTask
)

type State int

const (
	IDLE State = iota
	IN_PROGRESS
	COMPLETE
)

// type definition of task
type Task struct {
	ID       int
	workerID int
	FileName string
	State    State
	Type     TaskType
}
