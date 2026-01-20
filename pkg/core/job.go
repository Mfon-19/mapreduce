package core

import (
	"errors"
	"sync"
	"time"
)

type Job struct {
	ID           string
	InputFiles   []string
	NReduce      int
	MapTasks     []Task
	ReduceTasks  []Task
	Phase        Phase
	mapDone      int
	reduceDone   int
	timeout      time.Duration
	mu           sync.Mutex
	PluginPath   string
	MapSymbol    string
	ReduceSymbol string
}

func NewJob(id string, inputFiles []string, nReduce int, timeout time.Duration, pluginURI, mapSym, reduceSym string) *Job {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	job := &Job{
		ID:           id,
		InputFiles:   inputFiles,
		NReduce:      nReduce,
		Phase:        MapPhase,
		timeout:      timeout,
		PluginPath:   pluginURI,
		MapSymbol:    mapSym,
		ReduceSymbol: reduceSym,
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

func (job *Job) NextTask(workerID string) (TaskSpec, bool) {
	job.mu.Lock()
	defer job.mu.Unlock()

	now := time.Now()
	switch job.Phase {
	case DonePhase:
		return TaskSpec{Type: TaskExit}, false
	case MapPhase:
		if task := job.pickLocked(&job.MapTasks, workerID, now); task != nil {
			return toSpec(*task), true
		}
	case ReducePhase:
		if task := job.pickLocked(&job.ReduceTasks, workerID, now); task != nil {
			return toSpec(*task), true
		}
	default:
		panic("unhandled default case")
	}
	return TaskSpec{}, false
}

func (job *Job) Complete(result TaskResult, workerID string) (advanced bool, err error) {
	job.mu.Lock()
	defer job.mu.Unlock()

	var tasks *[]Task
	switch result.Type {
	case MapTask:
		tasks = &job.MapTasks
	case ReduceTask:
		tasks = &job.ReduceTasks
	default:
		return false, errors.New("invalid task type")
	}

	if result.ID < 0 || result.ID >= len(*tasks) {
		return false, errors.New("task id out of range")
	}
	task := &(*tasks)[result.ID]

	if task.State != InProgress || task.Assignee != workerID {
		return false, nil
	}

	if result.OK {
		task.State = Completed
		task.Assignee = ""
		task.Deadline = time.Time{}
		if result.Type == MapTask {
			job.mapDone++
		} else {
			job.reduceDone++
		}
	} else {
		task.State = Idle
		task.Assignee = ""
		task.Deadline = time.Time{}
	}

	switch job.Phase {
	case MapPhase:
		if job.mapDone == len(job.MapTasks) {
			job.Phase = ReducePhase
			return true, nil
		}
	case ReducePhase:
		if job.reduceDone == len(job.ReduceTasks) {
			job.Phase = DonePhase
			return true, nil
		}
	default:
		panic("unhandled default case")
	}
	return false, nil
}

func (job *Job) RequeueTimedOut(now time.Time) (requeued int) {
	job.mu.Lock()
	defer job.mu.Unlock()

	requeued += job.requeueTimedOutLocked(&job.MapTasks, now)
	requeued += job.requeueTimedOutLocked(&job.ReduceTasks, now)
	return
}

func (job *Job) RenewLeases(workerID string, running []TaskSpec, now time.Time) int {
	job.mu.Lock()
	defer job.mu.Unlock()

	renewed := 0
	for _, spec := range running {
		var tasks *[]Task
		switch spec.Type {
		case MapTask:
			tasks = &job.MapTasks
		case ReduceTask:
			tasks = &job.ReduceTasks
		default:
			continue
		}

		if spec.ID < 0 || spec.ID >= len(*tasks) {
			continue
		}
		task := &(*tasks)[spec.ID]
		if task.State != InProgress || task.Assignee != workerID {
			continue
		}
		task.Deadline = now.Add(job.timeout)
		renewed++
	}
	return renewed
}

func (job *Job) PluginSpec() (jobID, uri, mapSym, reduceSym string) {
	return job.ID, job.PluginPath, job.MapSymbol, job.ReduceSymbol
}

func (job *Job) pickLocked(tasks *[]Task, workerID string, now time.Time) *Task {
	for i := range *tasks {
		task := &(*tasks)[i]
		if task.State == Idle {
			job.leaseLocked(task, workerID, now)
			return task
		}
	}

	for i := range *tasks {
		task := &(*tasks)[i]
		if task.State == InProgress && !task.Deadline.IsZero() && now.After(task.Deadline) {
			job.leaseLocked(task, workerID, now)
			return task
		}
	}
	return nil
}

func (job *Job) leaseLocked(task *Task, workerID string, now time.Time) {
	task.State = InProgress
	task.Assignee = workerID
	task.StartTime = now
	task.Attempts++
	task.Deadline = now.Add(job.timeout)
}

func (job *Job) requeueTimedOutLocked(tasks *[]Task, now time.Time) int {
	n := 0
	for i := range *tasks {
		task := &(*tasks)[i]
		if task.State == InProgress && !task.Deadline.IsZero() && now.After(task.Deadline) {
			task.State = Idle
			task.Assignee = ""
			task.Deadline = time.Time{}
			n++
		}
	}
	return n
}

func toSpec(task Task) TaskSpec {
	return TaskSpec{
		ID:       task.ID,
		Type:     task.Type,
		Filename: task.Filename,
	}
}
