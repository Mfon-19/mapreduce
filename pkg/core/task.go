package core

import (
	"time"
)

type Task struct {
	ID        int
	Type      TaskType
	Filename  string
	State     TaskState
	StartTime time.Time
	Assignee  string
	Attempts  int
	Deadline  time.Time
}

type TaskSpec struct {
	ID       int
	Type     TaskType
	Filename string
}

type TaskResult struct {
	Type       TaskType
	ID         int
	OK         bool
	MapOutputs []string
}
