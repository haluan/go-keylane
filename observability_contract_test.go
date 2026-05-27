// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestStableMetricDescriptors(t *testing.T) {
	descs := StableMetricDescriptors()
	if len(descs) == 0 {
		t.Fatal("expected non-empty stable metric inventory")
	}
	seen := make(map[string]struct{}, len(descs))
	for _, d := range descs {
		if d.Stability != ObservabilityStable {
			t.Errorf("metric %q: stability = %q, want stable", d.Name, d.Stability)
		}
		if !strings.HasPrefix(d.Name, "keylane_") {
			t.Errorf("metric %q: must use keylane_ prefix", d.Name)
		}
		if _, dup := seen[d.Name]; dup {
			t.Errorf("duplicate metric name %q", d.Name)
		}
		seen[d.Name] = struct{}{}
		if strings.HasSuffix(d.Name, "_total") {
			continue
		}
		if strings.HasSuffix(d.Name, "_seconds") {
			continue
		}
		// gauges and summaries without _total/_seconds are allowed
	}
}

func TestStableMetricsExposeExpectedLabels(t *testing.T) {
	allowed := make(map[string]struct{}, len(AllowedDefaultMetricLabelNames()))
	for _, n := range AllowedDefaultMetricLabelNames() {
		allowed[n] = struct{}{}
	}
	for _, d := range StableMetricDescriptors() {
		for _, lbl := range d.Labels {
			if _, ok := allowed[lbl.Name]; !ok {
				t.Errorf("metric %q: label %q not in AllowedDefaultMetricLabelNames", d.Name, lbl.Name)
			}
			if lbl.Stability != ObservabilityStable {
				t.Errorf("metric %q label %q: want stable label stability", d.Name, lbl.Name)
			}
		}
	}
}

func TestForbiddenMetricLabelsNotInStableDescriptors(t *testing.T) {
	forbidden := ForbiddenMetricLabelNames()
	for _, d := range StableMetricDescriptors() {
		for _, lbl := range d.Labels {
			if slices.Contains(forbidden, lbl.Name) {
				t.Errorf("stable metric %q uses forbidden label %q", d.Name, lbl.Name)
			}
		}
	}
}

func TestExperimentalMetricPatternsForbiddenLabels(t *testing.T) {
	forbidden := ForbiddenMetricLabelNames()
	for _, d := range ExperimentalMetricPatterns() {
		if d.Stability != ObservabilityExperimental {
			t.Errorf("pattern %q: want experimental stability", d.Name)
		}
		for _, lbl := range d.Labels {
			if slices.Contains(forbidden, lbl.Name) {
				t.Errorf("experimental pattern %q uses forbidden label %q", d.Name, lbl.Name)
			}
		}
	}
}

func TestDebugSnapshotVersionedShape(t *testing.T) {
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 8,
		LaneQuotas:       map[Lane]int{"default": 1},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	snap := q.DebugSnapshot()
	if snap.Version != DebugSnapshotVersion {
		t.Errorf("Version = %q, want %q", snap.Version, DebugSnapshotVersion)
	}
	if snap.ShardCount != 1 {
		t.Errorf("ShardCount = %d, want 1", snap.ShardCount)
	}
	if snap.GeneratedAt.IsZero() {
		t.Error("GeneratedAt must be set")
	}
}

func TestObservabilityConfigValidationWarnings(t *testing.T) {
	t.Run("unset observability", func(t *testing.T) {
		report := ValidateConfig(Config{
			ShardCount:       1,
			WorkerCount:      1,
			QueueSizePerLane: 8,
			LaneQuotas:       map[Lane]int{"default": 1},
		})
		if !validationReportHasWarningCode(report, CodeConfigObservabilityFullDefaultsResolved) {
			t.Fatalf("expected warning %s", CodeConfigObservabilityFullDefaultsResolved)
		}
	})
	t.Run("raw request identifiers in hooks", func(t *testing.T) {
		report := ValidateConfig(Config{
			ShardCount:       1,
			WorkerCount:      1,
			QueueSizePerLane: 8,
			LaneQuotas:       map[Lane]int{"default": 1},
			Observability: ObservabilityConfig{
				EnableHooks:                 true,
				ExposeRawRequestIdentifiers: true,
			},
		})
		if !validationReportHasWarningCode(report, CodeConfigRawRequestIdentifiersInHooks) {
			t.Fatalf("expected warning %s", CodeConfigRawRequestIdentifiersInHooks)
		}
	})
	t.Run("raw key exposure", func(t *testing.T) {
		report := ValidateConfig(Config{
			ShardCount:       1,
			WorkerCount:      1,
			QueueSizePerLane: 8,
			LaneQuotas:       map[Lane]int{"default": 1},
			HotKey:           HotKeyConfig{Enabled: true, ExposeRawKey: true},
		})
		if !validationReportHasWarningCode(report, CodeConfigRawKeyExposureEnabled) {
			t.Fatalf("expected warning %s", CodeConfigRawKeyExposureEnabled)
		}
	})
	t.Run("debug snapshot hot path heavy", func(t *testing.T) {
		report := ValidateConfig(Config{
			ShardCount:       1,
			WorkerCount:      1,
			QueueSizePerLane: 8,
			LaneQuotas:       map[Lane]int{"default": 1},
			Observability: ObservabilityConfig{
				EnableDebugSnapshot:   true,
				EnableQueueWaitTiming: true,
				EnableRunTiming:       true,
			},
		})
		if !validationReportHasWarningCode(report, CodeConfigDebugSnapshotHotPathHeavy) {
			t.Fatalf("expected warning %s", CodeConfigDebugSnapshotHotPathHeavy)
		}
	})
}

func validationReportHasWarningCode(r ValidationReport, code string) bool {
	for _, issue := range r.Issues {
		if issue.Severity == ValidationWarning && issue.Code == code {
			return true
		}
	}
	return false
}

func TestExperimentalMetricPatternsInventory(t *testing.T) {
	patterns := ExperimentalMetricPatterns()
	if len(patterns) < 20 {
		t.Fatalf("patterns = %d, want full hook-adapter inventory", len(patterns))
	}
	names := make(map[string]struct{}, len(patterns))
	for _, p := range patterns {
		names[p.Name] = struct{}{}
	}
	for _, want := range []string{
		"keylane_backend_held_duration_seconds",
		"keylane_backend_admission_accepted_total",
		"keylane_backend_admission_rejected_total",
		"keylane_backend_in_use",
		"keylane_backend_wait_total",
		"keylane_backend_saturated",
	} {
		if _, ok := names[want]; !ok {
			t.Errorf("missing experimental pattern %q", want)
		}
	}
}

func TestHookPanicsRecoveredDiagnostic(t *testing.T) {
	before := HookPanicsRecovered()
	callHook(func() { panic("contract diagnostic") })
	if HookPanicsRecovered() != before+1 {
		t.Fatalf("HookPanicsRecovered = %d, want %d", HookPanicsRecovered(), before+1)
	}
}

func TestHookPanicDoesNotKillWorkerContract(t *testing.T) {
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
		Observability: ObservabilityConfig{
			SlowJobThreshold: time.Millisecond,
			Hooks: Hooks{
				OnJobTiming: func(JobTimingEvent) { panic("observer panic") },
			},
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	for i := 0; i < 3; i++ {
		done := make(chan struct{})
		_ = q.Submit(context.Background(), Job{
			Key:  "key",
			Lane: "default",
			Run: func(ctx context.Context) error {
				time.Sleep(2 * time.Millisecond)
				close(done)
				return nil
			},
		})
		<-done
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = q.Stop(stopCtx, WithDrain(true))

	if q.StatsGCPressure().Run.Count != 3 {
		t.Errorf("Run.Count = %d, want 3 after hook panics", q.StatsGCPressure().Run.Count)
	}
}
