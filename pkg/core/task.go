package core

import "time"

type Task struct {
	ID        int
	Type      TaskType
	Filename  string
	State     TaskState
	StartTime time.Time
}
