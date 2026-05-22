// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks_test

import (
	"sync"
)

// fifoBaseline is a minimal global FIFO scheduler for fairness comparison only.
// It is not a production API.
type fifoBaseline struct {
	jobs chan func()
	wg   sync.WaitGroup
}

func newFifoBaseline(workerCount, buffer int) *fifoBaseline {
	f := &fifoBaseline{
		jobs: make(chan func(), buffer),
	}
	for i := 0; i < workerCount; i++ {
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			for job := range f.jobs {
				if job != nil {
					job()
				}
			}
		}()
	}
	return f
}

func (f *fifoBaseline) submit(job func()) {
	f.jobs <- job
}

func (f *fifoBaseline) close() {
	close(f.jobs)
	f.wg.Wait()
}
