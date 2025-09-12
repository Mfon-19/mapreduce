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
