package mr

import "time"

// enum of different task type
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	ExitTask
	NoTask
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
	timer    *time.Timer
}
