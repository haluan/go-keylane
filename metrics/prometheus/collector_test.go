// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package prometheus

import (
	"context"
	"strings"
	"testing"

	"github.com/haluan/go-keylane"
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func testQueue(t *testing.T) *keylane.Queue {
	t.Helper()
	cfg := keylane.Config{
		ShardCount:       2,
		WorkerCount:      2,
		QueueSizePerLane: 100,
		LaneQuotas:       map[keylane.Lane]int{"default": 2},
	}
	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return q
}

func TestNewCollectorRegistersAndCollects(t *testing.T) {
	q := testQueue(t)
	reg := prom.NewRegistry()
	if err := reg.Register(NewCollector(q, CollectorOptions{SchedulerName: "test"})); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = q.Submit(ctx, keylane.Job{
			Key:  "k",
			Lane: "default",
			Run:  func(ctx context.Context) error { return nil },
		})
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(mfs) == 0 {
		t.Fatal("expected metric families")
	}
	names := make(map[string]struct{})
	for _, mf := range mfs {
		names[mf.GetName()] = struct{}{}
		if strings.Contains(mf.String(), `lane="key"`) || strings.Contains(mf.String(), `key="`) {
			t.Error("metric must not include job key labels")
		}
	}
	for _, want := range []string{
		"keylane_jobs_submitted_total",
		"keylane_lane_depth",
		"keylane_pressure_ratio",
		"keylane_queue_wait_seconds",
		"keylane_run_duration_seconds",
	} {
		if _, ok := names[want]; !ok {
			t.Errorf("missing metric family %q", want)
		}
	}
}

func TestCollectorDefaultSchedulerName(t *testing.T) {
	q := testQueue(t)
	c := NewCollector(q, CollectorOptions{})
	ch := make(chan *prom.Desc, 32)
	c.Describe(ch)
	close(ch)
	if len(ch) != len(allDescriptors()) {
		t.Errorf("Describe count = %d, want %d", len(ch), len(allDescriptors()))
	}
}

func TestCollectorNoPanicWhenStatsDisabled(t *testing.T) {
	cfg := keylane.Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[keylane.Lane]int{"default": 1},
		Observability: keylane.ObservabilityConfig{
			EnableStats:    false,
			EnableCounters: true,
		},
	}
	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	c := NewCollector(q, CollectorOptions{SchedulerName: "empty"})
	ch := make(chan prom.Metric, 64)
	c.Collect(ch)
	close(ch)
	for range ch {
	}
}

func TestCollectorSubmittedCounter(t *testing.T) {
	q := testQueue(t)
	c := NewCollector(q, CollectorOptions{SchedulerName: "c"})
	_ = q.Submit(context.Background(), keylane.Job{
		Key: "k", Lane: "default",
		Run: func(ctx context.Context) error { return nil },
	})

	var found bool
	ch := make(chan prom.Metric, 128)
	c.Collect(ch)
	close(ch)
	for m := range ch {
		if strings.Contains(m.Desc().String(), "jobs_submitted_total") {
			found = true
		}
	}
	if !found {
		t.Error("expected jobs_submitted_total metric")
	}
}

func TestCollectorTimingSummaries(t *testing.T) {
	q := testQueue(t)
	c := NewCollector(q, CollectorOptions{SchedulerName: "timing"})
	done := make(chan struct{}, 3)
	for i := 0; i < 3; i++ {
		if err := q.Submit(context.Background(), keylane.Job{
			Key:  "k",
			Lane: "default",
			Run: func(ctx context.Context) error {
				done <- struct{}{}
				return nil
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		<-done
	}

	reg := prom.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatal(err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var queueWait, runDur *dto.MetricFamily
	for _, mf := range mfs {
		switch mf.GetName() {
		case "keylane_queue_wait_seconds":
			queueWait = mf
		case "keylane_run_duration_seconds":
			runDur = mf
		}
	}
	if queueWait == nil || runDur == nil {
		t.Fatalf("missing summary families: queue_wait=%v run=%v", queueWait != nil, runDur != nil)
	}
	if queueWait.GetType() != dto.MetricType_SUMMARY || runDur.GetType() != dto.MetricType_SUMMARY {
		t.Fatalf("want SUMMARY type, got queue_wait=%v run=%v", queueWait.GetType(), runDur.GetType())
	}
	for _, mf := range []*dto.MetricFamily{queueWait, runDur} {
		for _, m := range mf.GetMetric() {
			s := m.GetSummary()
			if s == nil {
				continue
			}
			if s.GetSampleCount() == 0 {
				continue
			}
			if s.GetSampleSum() <= 0 {
				t.Errorf("%s: expected positive sample_sum, got %v", mf.GetName(), s.GetSampleSum())
			}
		}
	}
}

func TestCollectorDescribeCount(t *testing.T) {
	q := testQueue(t)
	c := NewCollector(q, CollectorOptions{})
	ch := make(chan *prom.Desc, 32)
	c.Describe(ch)
	close(ch)
	n := 0
	for range ch {
		n++
	}
	if n != len(allDescriptors()) {
		t.Errorf("Describe emitted %d descriptors, want %d", n, len(allDescriptors()))
	}
}
