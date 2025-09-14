package coordinator

import (
	"context"
	"errors"
	"log"
	"mapreduce/pkg/core"
	"sync"
	"time"
)

type Master struct {
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	job         *core.Job
	log         *log.Logger
	sweeperStop chan struct{}
}

func NewMaster(ctx context.Context, log *log.Logger) *Master {
	if ctx == nil {
		ctx = context.Background()
	}
	c, cancel := context.WithCancel(ctx)
	master := &Master{
		ctx:         c,
		cancel:      cancel,
		log:         log,
		sweeperStop: make(chan struct{}),
	}
	return master
}

func (master *Master) Close() {
	master.cancel()
	close(master.sweeperStop)
}

func (master *Master) SubmitJob(id string, inputs []string, nReduce int, leaseTimeout time.Duration) error {
	master.mu.Lock()
	defer master.mu.Unlock()
	if master.job != nil && master.job.Phase != core.DonePhase {
		return errors.New("job already running")
	}
	master.job = core.NewJob(id, inputs, nReduce, leaseTimeout)
	// TODO: I want to return the job id as well. maybe
	return nil
}

func (master *Master) PollTask(workerID string) (core.TaskSpec, bool) {
	master.mu.Lock()
	job := master.job
	master.mu.Unlock()

	if job == nil {
		return core.TaskSpec{Type: core.TaskExit}, false
	}
	return job.NextTask(workerID)
}

func (master *Master) ReportTaskResult(workerID string, result core.TaskResult) (done bool, err error) {
	master.mu.Lock()
	job := master.job
	master.mu.Unlock()

	if job == nil {
		return false, errors.New("no active job")
	}
	advanced, err := job.Complete(result, workerID)
	if err != nil {
		return false, err
	}

	if advanced {
		master.mu.Lock()
		defer master.mu.Unlock()
		if job.Phase == core.DonePhase {
			master.job = nil
			return true, nil
		}
	}
	return false, nil
}

func (master *Master) Heartbeat(workerID string, running []core.TaskSpec) {
	// TODO: Renew the lease for this worker
}

func (master *Master) leaseSweeper(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-master.sweeperStop:
			return
		case <-master.ctx.Done():
			return
		case now := <-t.C:
			master.mu.Lock()
			job := master.job
			master.mu.Unlock()
			if job == nil {
				continue
			}
			n := job.RequeueTimedOut(now)
			if n > 0 && master.log != nil {
				master.log.Printf("requeued %d timed-out tasks", n)
			}
		}
	}
}
