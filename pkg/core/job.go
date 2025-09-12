package core

import (
	"errors"
	"sync"
	"time"
)

type Job struct {
	ID          string
	InputFiles  []string
	NReduce     int
	MapTasks    []Task
	ReduceTasks []Task
	Phase       Phase
	timeout     time.Duration
	mu          sync.Mutex
}

func NewJob(id string, inputFiles []string, nReduce int) *Job {
	job := &Job{
		ID:         id,
		InputFiles: inputFiles,
		NReduce:    nReduce,
		Phase:      MapPhase,
	}

	nMap := len(inputFiles)
	job.MapTasks = make([]Task, nMap)
	for i := 0; i < nMap; i++ {
		job.MapTasks[i] = Task{
			ID:       i,
			Type:     MapTask,
			Filename: inputFiles[i],
			State:    Idle,
		}
	}

	job.ReduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		job.ReduceTasks[i] = Task{
			ID:    i,
			Type:  ReduceTask,
			State: Idle,
		}
	}

	return job
}

func findNextTask(job *Job) *Task {
	job.mu.Lock()
	defer job.mu.Unlock()

	if job.Phase == DonePhase {
		return nil
	}

	task := &Task{}
	if job.Phase == MapPhase {
		task = findNextMapTask(job)
	}

	if job.Phase == ReducePhase {
		task = findNextReduceTask(job)
	}

	return task
}

func findNextMapTask(job *Job) *Task {
	task := findIdleTask(job.MapTasks)
	if task == nil {
		task = findTimedOutTask(job.MapTasks, job.timeout)
	}

	task.State = InProgress
	task.StartTime = time.Now()
	return task
}

func findNextReduceTask(job *Job) *Task {
	task := findIdleTask(job.ReduceTasks)
	if task == nil {
		task = findTimedOutTask(job.ReduceTasks, job.timeout)
	}
	task.State = InProgress
	task.StartTime = time.Now()
	return task
}

func findIdleTask(tasks []Task) *Task {
	for _, task := range tasks {
		if task.State == Idle {
			return &task
		}
	}
	return nil
}

func findTimedOutTask(tasks []Task, timeout time.Duration) *Task {
	for _, task := range tasks {
		if task.State == InProgress && time.Now().Sub(task.StartTime) > timeout {
			return &task
		}
	}
	return nil
}

func advancePhase(job *Job) error {
	if findNextTask(job) != nil {
		return errors.New("there are still tasks to do")
	}

	if job.Phase == MapPhase {
		job.Phase = ReducePhase
	} else if job.Phase == ReducePhase {
		job.Phase = DonePhase
	}
	return nil
}

func updateTaskStatus(job *Job, task *Task) {
	job.mu.Lock()
	defer job.mu.Unlock()

	task.State = Completed
}
