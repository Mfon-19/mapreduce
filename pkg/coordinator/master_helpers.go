package coordinator

import "mapreduce/pkg/core"

func (master *Master) Active() bool {
	master.mu.Lock()
	defer master.mu.Unlock()
	return master.job != nil && master.job.Phase != core.DonePhase
}

func (master *Master) Totals() (int, int) {
	master.mu.Lock()
	defer master.mu.Unlock()
	if master.job == nil {
		return 0, 0
	}
	return len(master.job.MapTasks), len(master.job.ReduceTasks)
}
