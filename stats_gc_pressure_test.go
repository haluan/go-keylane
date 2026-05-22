// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

func maxStatsGCPressureQueued(cfg keylane.Config) uint64 {
	laneCount := len(cfg.LaneQuotas)
	return uint64(cfg.ShardCount) * uint64(laneCount) * uint64(cfg.QueueSizePerLane)
}

func maxStatsGCPressureInFlight(cfg keylane.Config) uint64 {
	var totalQuota int
	for _, quota := range cfg.LaneQuotas {
		totalQuota += quota
	}
	return uint64(cfg.WorkerCount) * uint64(totalQuota)
}

func checkStatsGCPressureSane(cfg keylane.Config, snap keylane.StatsGCPressureSnapshot) error {
	maxQueued := maxStatsGCPressureQueued(cfg)
	maxInFlight := maxStatsGCPressureInFlight(cfg)

	if snap.TotalQueued > maxQueued {
		return errors.New("TotalQueued exceeds configured capacity")
	}
	if snap.TotalInFlight > maxInFlight {
		return errors.New("TotalInFlight exceeds worker * quota bound")
	}

	for _, shard := range snap.Shards {
		if shard.Queued > maxQueued {
			return errors.New("shard Queued exceeds configured capacity")
		}
		if shard.InFlight > maxInFlight {
			return errors.New("shard InFlight exceeds worker * quota bound")
		}
		for _, pl := range shard.PerLane {
			if pl.Queued > maxQueued {
				return errors.New("PerLane Queued exceeds configured capacity")
			}
		}
	}
	for _, lane := range snap.Lanes {
		if lane.Queued > maxQueued {
			return errors.New("lane Queued exceeds configured capacity")
		}
		if lane.InFlight > maxInFlight {
			return errors.New("lane InFlight exceeds worker * quota bound")
		}
		c := lane.Counters
		// Admission counters may be briefly skewed across separate atomic loads under
		// concurrency; allow one in-flight submit (Submitted++) before Accepted/Rejected.
		if c.Submitted+1 < c.Accepted+c.Rejected {
			return errors.New("lane Submitted < Accepted + Rejected")
		}
		if c.QueueFull > c.Rejected+1 {
			return errors.New("lane QueueFull exceeds Rejected")
		}
		terminal := c.Completed + c.Failed + c.Canceled + c.Panicked
		if terminal > c.Accepted+1 {
			return errors.New("lane terminal outcomes exceed Accepted")
		}
	}
	return nil
}

func assertStatsGCPressureSane(t *testing.T, cfg keylane.Config, snap keylane.StatsGCPressureSnapshot) {
	t.Helper()
	if err := checkStatsGCPressureSane(cfg, snap); err != nil {
		t.Error(err)
	}
}

func TestStatsGCPressureEmptyQueue(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       2,
		WorkerCount:      3,
		QueueSizePerLane: 5,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	snap := q.StatsGCPressure()
	if snap.Version != keylane.StatsGCPressureVersion {
		t.Errorf("Version = %q, want %q", snap.Version, keylane.StatsGCPressureVersion)
	}
	if snap.ShardCount != 2 || snap.LaneCount != 1 || snap.WorkerCount != 3 {
		t.Errorf("counts: shards=%d lanes=%d workers=%d", snap.ShardCount, snap.LaneCount, snap.WorkerCount)
	}
	if snap.TotalQueued != 0 || snap.TotalInFlight != 0 {
		t.Errorf("expected zero totals, got queued=%d inflight=%d", snap.TotalQueued, snap.TotalInFlight)
	}
}

func TestStatsGCPressureQueuedJobs(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	block := make(chan struct{})
	defer close(block)
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			<-block
			return nil
		},
	})
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 2; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	snap := q.StatsGCPressure()
	if snap.TotalQueued != 2 {
		t.Errorf("TotalQueued = %d, want 2", snap.TotalQueued)
	}
	if snap.Shards[0].Queued != 2 {
		t.Errorf("shard queued = %d, want 2", snap.Shards[0].Queued)
	}
	if snap.Lanes[0].Queued != 2 {
		t.Errorf("lane queued = %d, want 2", snap.Lanes[0].Queued)
	}
}

func TestStatsGCPressureInFlightJobs(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	block := make(chan struct{})
	started := make(chan struct{})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run: func(ctx context.Context) error {
			close(started)
			<-block
			return nil
		},
	})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}

	snap := q.StatsGCPressure()
	if snap.TotalInFlight != 1 {
		t.Errorf("TotalInFlight = %d, want 1", snap.TotalInFlight)
	}
	if snap.Lanes[0].InFlight != 1 {
		t.Errorf("lane InFlight = %d, want 1", snap.Lanes[0].InFlight)
	}
	close(block)
}

func TestStatsGCPressureInFlightReturnsToZero(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := q.Stop(stopCtx, keylane.WithDrain(true)); err != nil {
		t.Fatalf("Stop with drain: %v", err)
	}

	snap := q.StatsGCPressure()
	if snap.TotalInFlight != 0 {
		t.Errorf("TotalInFlight = %d, want 0 after drain", snap.TotalInFlight)
	}
	if snap.TotalQueued != 0 {
		t.Errorf("TotalQueued = %d, want 0 after drain", snap.TotalQueued)
	}
}

func TestStatsGCPressureDoesNotExposeInternalMutableState(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})

	snap1 := q.StatsGCPressure()
	snap1.Shards[0].PerLane[0].Queued = 999
	snap1.Lanes[0].Counters.Submitted = 999

	snap2 := q.StatsGCPressure()
	if snap2.Shards[0].PerLane[0].Queued != 1 {
		t.Error("StatsGCPressure leaked internal mutable array references")
	}
	if snap2.Lanes[0].Counters.Submitted != 1 {
		t.Error("StatsGCPressure leaked internal counter values")
	}
}

func TestStatsGCPressureQueueFullState(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 2,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	var lastErr error
	for i := 0; i < 5; i++ {
		lastErr = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	if !errors.Is(lastErr, keylane.ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", lastErr)
	}

	snap := q.StatsGCPressure()
	assertStatsGCPressureSane(t, cfg, snap)
	if snap.TotalQueued > 2 {
		t.Errorf("TotalQueued = %d, want at most 2", snap.TotalQueued)
	}
}

func TestQueueWaitGCPressureAcceptedJob(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = q.Stop(stopCtx, keylane.WithDrain(true))

	qw := q.StatsGCPressure().QueueWait
	if qw.Count != 1 {
		t.Fatalf("QueueWait Count = %d, want 1", qw.Count)
	}
}

func TestQueueWaitGCPressureQueueFullNoSample(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 2,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	for i := 0; i < 5; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	if q.StatsGCPressure().QueueWait.Count != 0 {
		t.Errorf("Count = %d, want 0 without worker execution", q.StatsGCPressure().QueueWait.Count)
	}
}

func sumLaneQueueWaitGCPressure(lanes []keylane.LaneStatsGCPressure) (count, total uint64) {
	for _, lane := range lanes {
		count += lane.QueueWait.Count
		total += lane.QueueWait.TotalNanos
	}
	return count, total
}

func TestQueueWaitGCPressureGlobalEqualsSumOfLanes(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"laneA": 1, "laneB": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key-a",
		Lane: "laneA",
		Run:  func(ctx context.Context) error { return nil },
	})
	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key-b",
		Lane: "laneB",
		Run:  func(ctx context.Context) error { return nil },
	})

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = q.Stop(stopCtx, keylane.WithDrain(true))

	snap := q.StatsGCPressure()
	laneCount, laneTotal := sumLaneQueueWaitGCPressure(snap.Lanes)

	if snap.QueueWait.Count != laneCount {
		t.Errorf("global Count = %d, sum of lanes = %d", snap.QueueWait.Count, laneCount)
	}
	if snap.QueueWait.TotalNanos != laneTotal {
		t.Errorf("global TotalNanos = %d, sum of lanes = %d", snap.QueueWait.TotalNanos, laneTotal)
	}
	if snap.Lanes[0].QueueWait.Count != 1 || snap.Lanes[1].QueueWait.Count != 1 {
		t.Errorf("per-lane Count: laneA=%d laneB=%d, want 1 each",
			snap.Lanes[0].QueueWait.Count, snap.Lanes[1].QueueWait.Count)
	}
}

func TestQueueWaitGCPressureAverageHelper(t *testing.T) {
	qw := keylane.QueueWaitStatsGCPressure{Count: 2, TotalNanos: 100, MaxNanos: 80}
	if qw.AverageNanos() != 50 {
		t.Errorf("AverageNanos = %d, want 50", qw.AverageNanos())
	}
	if qw.AverageDuration() != 50*time.Nanosecond {
		t.Errorf("AverageDuration = %v", qw.AverageDuration())
	}
	if qw.MaxDuration() != 80*time.Nanosecond {
		t.Errorf("MaxDuration = %v", qw.MaxDuration())
	}
}

func TestQueueWaitConcurrentSubmitAndStats(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      4,
		QueueSizePerLane: 50,
		LaneQuotas:       map[keylane.Lane]int{"default": 2},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = q.Submit(context.Background(), keylane.Job{
				Key:  "key",
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			snap := q.StatsGCPressure()
			recordErr(checkStatsGCPressureSane(cfg, snap))
			qw := snap.QueueWait
			if qw.Count > 0 && qw.MaxNanos > qw.TotalNanos {
				recordErr(errors.New("MaxNanos exceeds TotalNanos"))
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
}

func TestLaneCountersGCPressureSubmitAccepted(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	_ = q.Submit(context.Background(), keylane.Job{
		Key:  "key",
		Lane: "default",
		Run:  func(ctx context.Context) error { return nil },
	})

	c := q.StatsGCPressure().Lanes[0].Counters
	if c.Submitted != 1 || c.Accepted != 1 || c.Rejected != 0 {
		t.Errorf("counters = %+v, want Submitted=1 Accepted=1 Rejected=0", c)
	}
}

func TestLaneCountersGCPressureQueueFull(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 2,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
	}
	q, _ := keylane.New(cfg)

	for i := 0; i < 5; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key:  "key",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	c := q.StatsGCPressure().Lanes[0].Counters
	if c.Submitted < c.Accepted+c.Rejected {
		t.Errorf("Submitted %d < Accepted %d + Rejected %d", c.Submitted, c.Accepted, c.Rejected)
	}
	if c.QueueFull == 0 {
		t.Error("expected QueueFull > 0")
	}
}

func TestLaneCountersConcurrentSubmitRunAndStatsGCPressure(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      4,
		QueueSizePerLane: 50,
		LaneQuotas:       map[keylane.Lane]int{"default": 2, "fast": 1},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	lastSubmitted := make([]atomic.Uint64, 2)

	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			lane := "default"
			if i%3 == 0 {
				lane = "fast"
			}
			_ = q.Submit(context.Background(), keylane.Job{
				Key:  "key",
				Lane: keylane.Lane(lane),
				Run:  func(ctx context.Context) error { return nil },
			})
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			snap := q.StatsGCPressure()
			recordErr(checkStatsGCPressureSane(cfg, snap))
			for li, lane := range snap.Lanes {
				if li >= len(lastSubmitted) {
					break
				}
				sub := lane.Counters.Submitted
				prev := lastSubmitted[li].Load()
				if sub < prev {
					recordErr(errors.New("lane Submitted counter decreased"))
				} else if sub > prev {
					lastSubmitted[li].Store(sub)
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			snap := q.StatsGCPressure()
			recordErr(checkStatsGCPressureSane(cfg, snap))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
}

func TestStatsGCPressureConcurrentSubmitRunAndRead(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       4,
		WorkerCount:      4,
		QueueSizePerLane: 50,
		LaneQuotas:       map[keylane.Lane]int{"default": 2},
	}
	q, _ := keylane.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = q.Submit(context.Background(), keylane.Job{
				Key:  "key",
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			})
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			snap := q.StatsGCPressure()
			recordErr(checkStatsGCPressureSane(cfg, snap))
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = q.Stats()
			snap := q.StatsGCPressure()
			recordErr(checkStatsGCPressureSane(cfg, snap))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
}
