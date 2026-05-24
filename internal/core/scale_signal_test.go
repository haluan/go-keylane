// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"sync"
	"testing"
	"time"
)

func testAutoscalingCfg() AutoscalingSignalConfig {
	return AutoscalingSignalConfig{
		Enabled:                       true,
		Window:                        time.Second,
		ConsecutiveWindows:            2,
		QueueDepthRatioThreshold:      0.70,
		QueueWaitMaxThreshold:         50 * time.Millisecond,
		AdmissionRejectRateThreshold:  0.05,
		AdmissionShedRateThreshold:    0.01,
		WorkerBusyRatioThreshold:      0.85,
		HotShardRatioThreshold:        0.70,
		ManyHotShardsThreshold:        4,
		LocalizedHotKeyRatioThreshold: 0.40,
	}
}

func TestScaleSignalHealthy(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{
		QueueDepthTotal: 10, QueueCapacityTotal: 100,
		QueueWaitMax:    2 * time.Millisecond,
		WorkerBusyRatio: 0.3,
	}
	now := time.Now()
	sig := calc.calculate(in, admissionCounterSample{submitted: 1}, now)
	if sig.Recommended {
		t.Fatal("expected no recommendation when healthy")
	}
	if sig.Reason != ScaleReasonNone {
		t.Fatalf("reason = %q, want none", sig.Reason)
	}
	if !sig.DiagnosticsEnabled {
		t.Fatal("expected diagnostics enabled")
	}
}

func TestScaleSignalQueueDepthHigh(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{
		QueueDepthTotal: 80, QueueCapacityTotal: 100,
	}
	now := time.Now()
	calc.lastSampleAt = now.Add(-2 * time.Second)
	counters := admissionCounterSample{submitted: 100}
	sig1 := calc.calculate(in, counters, now)
	if sig1.Reason != ScaleReasonQueueDepthHigh {
		t.Fatalf("reason = %q, want queue_depth_high", sig1.Reason)
	}
	if sig1.Recommended {
		t.Fatal("first window should not recommend yet")
	}
	sig2 := calc.calculate(in, counters, now.Add(time.Second))
	if !sig2.Recommended {
		t.Fatal("expected recommendation after consecutive unhealthy windows")
	}
}

func TestScaleSignalQueueDepthHighSingleShardScope(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{
		QueueDepthTotal: 80, QueueCapacityTotal: 100,
		HotShardCount: 1,
	}
	calc.lastSampleAt = time.Now().Add(-2 * time.Second)
	sig := calc.calculate(in, admissionCounterSample{submitted: 10}, time.Now())
	if sig.Scope != ScaleScopeShard {
		t.Fatalf("scope = %q, want shard", sig.Scope)
	}
}

func TestScaleSignalManyHotShardsGlobalScope(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{
		QueueDepthTotal: 10, QueueCapacityTotal: 100,
		HotShardCount: 5,
		HotShardRatio: 0.80,
	}
	calc.lastSampleAt = time.Now().Add(-2 * time.Second)
	sig := calc.calculate(in, admissionCounterSample{submitted: 10}, time.Now())
	if sig.Scope != ScaleScopeGlobal {
		t.Fatalf("scope = %q, want global", sig.Scope)
	}
}

func TestScaleSignalQueueWaitHigh(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{
		QueueDepthTotal: 10, QueueCapacityTotal: 100,
		QueueWaitMax: 80 * time.Millisecond,
	}
	calc.lastSampleAt = time.Now().Add(-2 * time.Second)
	sig := calc.calculate(in, admissionCounterSample{submitted: 10}, time.Now())
	if sig.Reason != ScaleReasonQueueWaitHigh {
		t.Fatalf("reason = %q, want queue_wait_high", sig.Reason)
	}
}

func TestScaleSignalAdmissionRejectHigh(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{
		QueueDepthTotal: 10, QueueCapacityTotal: 100,
		AdmissionRejectedRate: 0.10,
	}
	calc.lastSampleAt = time.Now().Add(-2 * time.Second)
	sig := calc.calculate(in, admissionCounterSample{submitted: 10}, time.Now())
	if sig.Reason != ScaleReasonAdmissionRejectHigh {
		t.Fatalf("reason = %q, want admission_reject_high", sig.Reason)
	}
}

func TestScaleSignalAdmissionShedHigh(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{
		QueueDepthTotal: 10, QueueCapacityTotal: 100,
		AdmissionShedRate: 0.05,
	}
	calc.lastSampleAt = time.Now().Add(-2 * time.Second)
	sig := calc.calculate(in, admissionCounterSample{submitted: 10}, time.Now())
	if sig.Reason != ScaleReasonAdmissionShedHigh {
		t.Fatalf("reason = %q, want admission_shed_high", sig.Reason)
	}
}

func TestScaleSignalWorkerSaturated(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{
		QueueDepthTotal: 10, QueueCapacityTotal: 100,
		WorkerBusyRatio: 0.95,
	}
	calc.lastSampleAt = time.Now().Add(-2 * time.Second)
	sig := calc.calculate(in, admissionCounterSample{submitted: 10}, time.Now())
	if sig.Reason != ScaleReasonWorkerSaturated {
		t.Fatalf("reason = %q, want worker_saturated", sig.Reason)
	}
}

func TestScaleSignalManyHotShards(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{
		QueueDepthTotal: 10, QueueCapacityTotal: 100,
		HotShardCount: 5,
		HotShardRatio: 0.80,
	}
	calc.lastSampleAt = time.Now().Add(-2 * time.Second)
	sig := calc.calculate(in, admissionCounterSample{submitted: 10}, time.Now())
	if sig.Reason != ScaleReasonManyHotShards {
		t.Fatalf("reason = %q, want many_hot_shards", sig.Reason)
	}
}

func TestScaleSignalDistributedPressure(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{
		QueueDepthTotal:    10,
		QueueCapacityTotal: 100,
		ShardPressureClass: ShardPressureDistributed,
	}
	calc.lastSampleAt = time.Now().Add(-2 * time.Second)
	sig := calc.calculate(in, admissionCounterSample{submitted: 10}, time.Now())
	if sig.Reason != ScaleReasonDistributedPressure {
		t.Fatalf("reason = %q, want distributed_pressure", sig.Reason)
	}
}

func TestScaleSignalLocalizedHotKeyNotRecommended(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{
		QueueDepthTotal:      80,
		QueueCapacityTotal:   100,
		QueueWaitMax:         90 * time.Millisecond,
		WorkerBusyRatio:      0.95,
		HotShardCount:        1,
		LocalizedHotKeyRatio: 0.70,
		HotKeyCandidateCount: 1,
	}
	calc.unhealthyWindows = 5
	calc.lastSampleAt = time.Now().Add(-2 * time.Second)
	sig := calc.calculate(in, admissionCounterSample{submitted: 10}, time.Now())
	if sig.Recommended {
		t.Fatal("localized hot key should not recommend global scale")
	}
	if sig.Reason != ScaleReasonLocalizedHotKey {
		t.Fatalf("reason = %q, want localized_hot_key", sig.Reason)
	}
	if sig.Scope != ScaleScopeHotKey {
		t.Fatalf("scope = %q, want hot_key", sig.Scope)
	}
	if !sig.LocalizedHotKey {
		t.Fatal("expected LocalizedHotKey true")
	}
	snap := scaleSignalToSnapshot(sig)
	if !snap.LocalizedHotKey {
		t.Fatal("expected LocalizedHotKey on snapshot")
	}
}

func TestScaleSignalLocalizedHotKeyPreservesUnhealthyWindows(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	hotDistributed := scaleSignalInput{
		QueueDepthTotal:      80,
		QueueCapacityTotal:   100,
		HotShardCount:        8,
		HotShardRatio:        0.90,
		LocalizedHotKeyRatio: 0.70,
	}
	now := time.Now()
	calc.lastSampleAt = now.Add(-2 * time.Second)
	_ = calc.calculate(hotDistributed, admissionCounterSample{submitted: 100}, now)
	if calc.unhealthyWindows == 0 {
		t.Fatal("expected unhealthy windows after distributed pressure")
	}
	windowsBefore := calc.unhealthyWindows
	localized := scaleSignalInput{
		QueueDepthTotal:      80,
		QueueCapacityTotal:   100,
		HotShardCount:        1,
		LocalizedHotKeyRatio: 0.70,
	}
	calc.lastSampleAt = now.Add(-time.Second)
	_ = calc.calculate(localized, admissionCounterSample{submitted: 200}, now.Add(time.Second))
	if calc.unhealthyWindows < windowsBefore {
		t.Fatalf("unhealthyWindows = %d, want >= %d", calc.unhealthyWindows, windowsBefore)
	}
}

func TestScaleSignalRecoversAfterHealthy(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	hot := scaleSignalInput{QueueDepthTotal: 80, QueueCapacityTotal: 100}
	healthy := scaleSignalInput{QueueDepthTotal: 10, QueueCapacityTotal: 100}
	now := time.Now()
	counters := admissionCounterSample{submitted: 100}
	calc.lastSampleAt = now.Add(-2 * time.Second)
	_ = calc.calculate(hot, counters, now)
	calc.lastSampleAt = now.Add(-time.Second)
	sig := calc.calculate(hot, counters, now.Add(time.Second))
	if !sig.Recommended {
		t.Fatal("expected recommended after two hot windows")
	}
	calc.lastSampleAt = now.Add(time.Second)
	sig2 := calc.calculate(healthy, counters, now.Add(2*time.Second))
	if sig2.Recommended {
		t.Fatal("expected recovery after healthy window")
	}
	if calc.unhealthyWindows != 0 {
		t.Fatalf("unhealthyWindows = %d, want 0", calc.unhealthyWindows)
	}
}

func TestScaleSignalInsufficientDataZeroCapacity(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{QueueCapacityTotal: 0}
	sig := calc.calculate(in, admissionCounterSample{}, time.Now())
	if sig.Reason != ScaleReasonInsufficientData {
		t.Fatalf("reason = %q, want insufficient_data", sig.Reason)
	}
}

func TestScaleSignalInsufficientDataNoSubmissions(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{QueueDepthTotal: 0, QueueCapacityTotal: 100}
	sig := calc.calculate(in, admissionCounterSample{}, time.Now())
	if sig.Reason != ScaleReasonInsufficientData {
		t.Fatalf("reason = %q, want insufficient_data", sig.Reason)
	}
}

func TestScaleSignalDisabled(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 32, reg)
	sig := s.ScaleSignalSnapshot()
	if sig.Recommended || sig.Reason != ScaleReasonNone {
		t.Fatalf("disabled signal = %+v, want none", sig)
	}
	if sig.DiagnosticsEnabled {
		t.Fatal("expected DiagnosticsEnabled false when disabled")
	}
}

func TestScaleSignalConfigureConcurrent(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(2, 2, 32, reg)
	s.ConfigureAutoscalingSignal(testAutoscalingCfg())
	s.ConfigureHotKey(HotKeyConfig{Enabled: true, MaxTrackedKeysPerShard: 8, DetectionWindow: time.Minute, HotKeyDepthRatio: 0.3})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.ScaleSignalSnapshot()
			}
		}
	}()
	for i := 0; i < 20; i++ {
		cfg := testAutoscalingCfg()
		cfg.ConsecutiveWindows = 1 + (i % 3)
		s.ConfigureAutoscalingSignal(cfg)
	}
	close(stop)
	wg.Wait()
}

func TestScaleSignalPressureRatioBounded(t *testing.T) {
	t.Parallel()
	calc := scaleSignalCalculator{cfg: testAutoscalingCfg()}
	in := scaleSignalInput{
		QueueDepthTotal: 50, QueueCapacityTotal: 100,
		QueueWaitMax:    100 * time.Millisecond,
		WorkerBusyRatio: 0.9,
	}
	sig := calc.calculate(in, admissionCounterSample{submitted: 10}, time.Now())
	if sig.PressureRatio <= 0 {
		t.Fatalf("PressureRatio = %v, want positive", sig.PressureRatio)
	}
	if sig.PressureRatio > 10 {
		t.Fatalf("PressureRatio = %v, unexpectedly large", sig.PressureRatio)
	}
}

func TestAdmissionRatesDelta(t *testing.T) {
	t.Parallel()
	last := admissionCounterSample{submitted: 100, rejected: 5, shed: 1, throttled: 2}
	cur := admissionCounterSample{submitted: 200, rejected: 15, shed: 3, throttled: 4}
	rej, shed, thr := admissionRates(cur, last)
	if rej != 0.10 {
		t.Fatalf("reject rate = %v, want 0.10", rej)
	}
	if shed != 0.02 {
		t.Fatalf("shed rate = %v, want 0.02", shed)
	}
	if thr != 0.02 {
		t.Fatalf("throttle rate = %v, want 0.02", thr)
	}
}

func TestPerKeyThrottledTotalMonotonic(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 32, reg)
	s.ConfigureHotKey(HotKeyConfig{Enabled: true, MaxTrackedKeysPerShard: 8, DetectionWindow: time.Minute, HotKeyDepthRatio: 0.3})
	cfg := PerKeyAdmissionConfig{
		Enabled: true, DefaultAction: PerKeyMitigationThrottle,
		MaxQueuedPerKey: 1, PressureRatioThreshold: 0.01, MinStatus: HotKeyStatusCandidate,
	}
	_ = s.ConfigurePerKeyAdmission(cfg)

	keyHash := uint64(42)
	now := time.Now()
	hk := s.hotKeyTrackerForShard(0)
	for i := 0; i < 5; i++ {
		hk.observeSubmit(keyHash, 0, "", now)
		hk.observeEnqueue(keyHash, 0, "", now)
	}
	for i := 0; i < 3; i++ {
		dec := s.EvaluatePerKeyAdmissionWithConfig(0, keyHash, LaneID(0), cfg)
		if dec.Action != PerKeyMitigationThrottle {
			t.Fatalf("dec %d action = %q, want throttle", i, dec.Action)
		}
	}
	if s.PerKeyThrottledTotal() == 0 {
		t.Fatal("expected throttle counter > 0")
	}
	before := s.PerKeyThrottledTotal()
	_ = s.EvaluatePerKeyAdmissionWithConfig(0, keyHash, LaneID(0), cfg)
	if s.PerKeyThrottledTotal() <= before {
		t.Fatal("expected throttle counter to increase on repeated throttle decisions")
	}
}
