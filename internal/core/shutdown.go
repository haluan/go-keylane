package core

import (
	"context"
	"time"
)

// Stop terminates the scheduler, waiting for in-flight/queued jobs to complete if drain is true.
func (s *Scheduler) Stop(ctx context.Context, drain bool) error {
	s.mu.Lock()
	if s.state == stateStopped {
		s.mu.Unlock()
		return nil
	}
	if s.state == stateStopping {
		doneChan := s.stopDone
		s.mu.Unlock()
		select {
		case <-doneChan:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.state = stateStopping
	s.stopDone = make(chan struct{})
	s.mu.Unlock()

	if drain {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
	Loop:
		for {
			if s.isDrained() {
				break
			}
			select {
			case <-ctx.Done():
				break Loop
			case <-ticker.C:
			}
		}
	}

	if s.workerCancel != nil {
		s.workerCancel()
	}

	waitChan := make(chan struct{})
	go func() {
		s.workerWG.Wait()
		s.mu.Lock()
		s.state = stateStopped
		if s.stopDone != nil {
			select {
			case <-s.stopDone:
			default:
				close(s.stopDone)
			}
		}
		s.mu.Unlock()
		close(waitChan)
	}()

	select {
	case <-waitChan:
	case <-ctx.Done():
	}

	return ctx.Err()
}
