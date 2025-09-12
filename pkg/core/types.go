package core

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type TaskType int

const (
	TaskNone   TaskType = iota // no work yet; worker should wait and retry
	MapTask                    // do a map task
	ReduceTask                 // do a reduce task
	TaskExit                   // all done; worker should exit
)
