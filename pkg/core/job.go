package core

import "time"

type Task struct {
	Filename  string
	State     TaskState
	StartTime time.Time
}
