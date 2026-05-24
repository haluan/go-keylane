// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package prometheus

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/haluan/go-keylane"
	prom "github.com/prometheus/client_golang/prometheus"
)

func v05MetricsTestQueue(t *testing.T) *keylane.Queue {
	t.Helper()
	cfg := keylane.Config{
		ShardCount:        2,
		WorkerCount:       2,
		QueueSizePerLane:  100,
		LaneQuotas:        map[keylane.Lane]int{"default": 2, "payment": 2},
		HotKey:            keylane.DefaultHotKeyConfig(),
		PerKeyAdmission:   keylane.DefaultPerKeyAdmissionConfig(),
		ShardPressure:     keylane.DefaultShardPressureConfig(),
		AutoscalingSignal: keylane.DefaultAutoscalingSignalConfig(),
	}
	q, err := keylane.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return q
}

func warmPerKeyDecisionMetrics(t *testing.T, q *keylane.Queue) {
	t.Helper()
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i := 0; i < 40; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key: "metrics-hot-key", Lane: "default", Run: run,
		})
	}
}

func TestV05MetricsRequiredFamiliesPresent(t *testing.T) {
	q := v05MetricsTestQueue(t)
	warmPerKeyDecisionMetrics(t, q)
	c := NewCollector(q, CollectorOptions{SchedulerName: "v05"})
	reg := prom.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatal(err)
	}
	rawKey := "tenant-secret-key-999"
	_ = q.Submit(context.Background(), keylane.Job{
		Key: rawKey, Lane: "default",
		Run: func(context.Context) error { return nil },
	})
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]struct{}{}
	for _, mf := range mfs {
		names[mf.GetName()] = struct{}{}
	}
	for _, want := range []string{
		"keylane_hot_key_candidate_count",
		"keylane_hot_key_pressure_ratio",
		"keylane_hot_key_rejected_total",
		"keylane_per_key_admission_decisions_total",
		"keylane_per_key_mitigation_actions_total",
		"keylane_shard_pressure_ratio",
		"keylane_shard_queue_depth",
		"keylane_shard_depth",
		"keylane_scale_pressure_ratio",
		"keylane_scale_recommended",
		"keylane_queue_depth_ratio",
		"keylane_worker_busy_ratio",
		"keylane_admission_throttled_total",
	} {
		if _, ok := names[want]; !ok {
			t.Errorf("missing metric family %q", want)
		}
	}
}

func TestV05MetricsNoRawKeyLabels(t *testing.T) {
	q := v05MetricsTestQueue(t)
	c := NewCollector(q, CollectorOptions{SchedulerName: "privacy"})
	reg := prom.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatal(err)
	}
	rawKey := "super-secret-customer-key"
	for i := 0; i < 10; i++ {
		_ = q.Submit(context.Background(), keylane.Job{
			Key: rawKey, Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	allowedLabels := map[string]struct{}{
		"scheduler": {},
		"lane":      {},
		"shard_id":  {},
		"action":    {},
		"reason":    {},
		"scope":     {},
		"quantile":  {},
	}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetValue() == rawKey {
					t.Fatalf("metric %q exposes raw key in label %q", mf.GetName(), lp.GetName())
				}
				if lp.GetName() == "key_hash" {
					t.Fatalf("metric %q must not use key_hash label by default", mf.GetName())
				}
				if _, ok := allowedLabels[lp.GetName()]; !ok {
					t.Fatalf("metric %q has unexpected label %q", mf.GetName(), lp.GetName())
				}
			}
		}
		if strings.Contains(mf.GetName(), "scale_recommended") {
			for _, m := range mf.GetMetric() {
				for _, lp := range m.GetLabel() {
					if lp.GetName() != "scheduler" && lp.GetName() != "reason" && lp.GetName() != "scope" {
						t.Fatalf("scale_recommended unexpected label %q", lp.GetName())
					}
				}
			}
		}
	}
}

func TestV05MetricsCollectRace(t *testing.T) {
	q := v05MetricsTestQueue(t)
	c := NewCollector(q, CollectorOptions{SchedulerName: "race"})
	var wg sync.WaitGroup
	stop := make(chan struct{})
	defer close(stop)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				select {
				case <-stop:
					return
				default:
				}
				ch := make(chan prom.Metric, 128)
				c.Collect(ch)
				close(ch)
				for range ch {
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = q.Submit(context.Background(), keylane.Job{
					Key: "k-" + string(rune('a'+id)), Lane: "default",
					Run: func(context.Context) error { return nil },
				})
			}
		}(i)
	}
	wg.Wait()
}
