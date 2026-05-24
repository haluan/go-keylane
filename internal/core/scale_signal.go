// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"sync"
	"time"
)

type scaleSignalInput struct {
	QueueDepthTotal    int64
	QueueCapacityTotal int64

	QueueWaitMax time.Duration

	AdmissionRejectedRate  float64
	AdmissionShedRate      float64
	AdmissionThrottledRate float64

	WorkerBusyRatio float64

	HotShardCount   int
	TotalShardCount int
	HotShardRatio   float64

	HotKeyCandidateCount int
	LocalizedHotKeyRatio float64

	ShardPressureClass ShardPressureClass
}

type admissionCounterSample struct {
	submitted uint64
	rejected  uint64
	shed      uint64
	throttled uint64
}

type scaleSignalRawInput struct {
	scaleSignalInput
	counters admissionCounterSample
}

type scaleShardAggregates struct {
	hotShardCount        int
	hotShardRatio        float64
	hotKeyCandidateCount int
	localizedRatio       float64
	pressureClass        ShardPressureClass
}

type scaleSignalCalculator struct {
	cfg AutoscalingSignalConfig
	mu  sync.Mutex

	unhealthyWindows int
	lastSampleAt     time.Time
	lastCounters     admissionCounterSample
}

func (c *scaleSignalCalculator) calculate(in scaleSignalInput, counters admissionCounterSample, now time.Time) ScaleSignal {
	cfg := c.cfg
	windowStart := c.lastSampleAt
	if windowStart.IsZero() {
		windowStart = now
	}

	depthRatio := safeRatio(uint64(in.QueueDepthTotal), uint64(in.QueueCapacityTotal))
	sig := ScaleSignal{
		DiagnosticsEnabled:     true,
		QueueDepthRatio:        depthRatio,
		QueueWaitMax:           in.QueueWaitMax,
		AdmissionRejectedRate:  in.AdmissionRejectedRate,
		AdmissionShedRate:      in.AdmissionShedRate,
		AdmissionThrottledRate: in.AdmissionThrottledRate,
		WorkerBusyRatio:        in.WorkerBusyRatio,
		HotShardCount:          in.HotShardCount,
		HotShardRatio:          in.HotShardRatio,
		HotKeyCandidateCount:   in.HotKeyCandidateCount,
		LocalizedHotKeyRatio:   in.LocalizedHotKeyRatio,
		WindowStartedAt:        windowStart,
		WindowEndedAt:          now,
		Reason:                 ScaleReasonNone,
		Scope:                  ScaleScopeNone,
	}

	if in.QueueCapacityTotal <= 0 {
		sig.Reason = ScaleReasonInsufficientData
		sig.Scope = ScaleScopeUnknown
		sig.PressureRatio = depthRatio
		return sig
	}

	if counters.submitted == 0 {
		sig.Reason = ScaleReasonInsufficientData
		sig.Scope = ScaleScopeUnknown
		sig.PressureRatio = depthRatio
		return sig
	}

	sig.PressureRatio = computeScalePressureRatio(in, cfg, depthRatio)

	advanceWindow := c.lastSampleAt.IsZero() || now.Sub(c.lastSampleAt) >= cfg.Window

	localized := in.LocalizedHotKeyRatio >= cfg.LocalizedHotKeyRatioThreshold && in.HotShardCount <= 2
	reason, scope, unhealthy := evaluateScaleTriggers(in, cfg, depthRatio)

	if localized {
		if advanceWindow {
			if unhealthy {
				c.unhealthyWindows++
			} else {
				c.unhealthyWindows = 0
			}
			c.lastSampleAt = now
			c.lastCounters = counters
		}
		sig.Reason = ScaleReasonLocalizedHotKey
		sig.Scope = ScaleScopeHotKey
		sig.Recommended = false
		return sig
	}

	sig.Reason = reason
	sig.Scope = scope

	if advanceWindow {
		if unhealthy {
			c.unhealthyWindows++
		} else {
			c.unhealthyWindows = 0
		}
		c.lastSampleAt = now
		c.lastCounters = counters
	}

	if unhealthy && c.unhealthyWindows >= cfg.ConsecutiveWindows {
		sig.Recommended = true
	}
	return sig
}

func computeScalePressureRatio(in scaleSignalInput, cfg AutoscalingSignalConfig, depthRatio float64) float64 {
	waitRatio := 0.0
	if cfg.QueueWaitMaxThreshold > 0 {
		waitRatio = float64(in.QueueWaitMax) / float64(cfg.QueueWaitMaxThreshold)
	}
	return maxFloat64(
		depthRatio,
		waitRatio,
		in.AdmissionRejectedRate,
		in.AdmissionShedRate,
		in.AdmissionThrottledRate,
		in.WorkerBusyRatio,
		in.HotShardRatio,
	)
}

func scopeForScaleTrigger(reason ScaleReason, in scaleSignalInput) ScaleScope {
	switch reason {
	case ScaleReasonAdmissionRejectHigh, ScaleReasonAdmissionShedHigh, ScaleReasonDistributedPressure:
		return ScaleScopeGlobal
	case ScaleReasonManyHotShards:
		if in.HotShardCount <= 1 {
			return ScaleScopeShard
		}
		return ScaleScopeGlobal
	case ScaleReasonWorkerSaturated:
		if in.ShardPressureClass == ShardPressureWorkerBound {
			return ScaleScopeGlobal
		}
		if in.HotShardCount == 1 {
			return ScaleScopeShard
		}
		return ScaleScopeGlobal
	case ScaleReasonQueueWaitHigh, ScaleReasonQueueDepthHigh:
		if in.HotShardCount == 1 &&
			in.ShardPressureClass != ShardPressureDistributed &&
			in.ShardPressureClass != ShardPressureWorkerBound {
			return ScaleScopeShard
		}
		return ScaleScopeGlobal
	default:
		return ScaleScopeGlobal
	}
}

func evaluateScaleTriggers(in scaleSignalInput, cfg AutoscalingSignalConfig, depthRatio float64) (ScaleReason, ScaleScope, bool) {
	type trigger struct {
		reason   ScaleReason
		fired    bool
		priority int
	}
	var triggers []trigger

	if in.AdmissionShedRate >= cfg.AdmissionShedRateThreshold {
		triggers = append(triggers, trigger{ScaleReasonAdmissionShedHigh, true, 1})
	}
	if in.AdmissionRejectedRate >= cfg.AdmissionRejectRateThreshold {
		triggers = append(triggers, trigger{ScaleReasonAdmissionRejectHigh, true, 2})
	}
	if in.ShardPressureClass == ShardPressureDistributed || in.ShardPressureClass == ShardPressureWorkerBound {
		triggers = append(triggers, trigger{ScaleReasonDistributedPressure, true, 3})
	}
	if in.HotShardCount >= cfg.ManyHotShardsThreshold || in.HotShardRatio >= cfg.HotShardRatioThreshold {
		triggers = append(triggers, trigger{ScaleReasonManyHotShards, true, 4})
	}
	if in.WorkerBusyRatio >= cfg.WorkerBusyRatioThreshold || in.ShardPressureClass == ShardPressureWorkerBound {
		triggers = append(triggers, trigger{ScaleReasonWorkerSaturated, true, 5})
	}
	if cfg.QueueWaitMaxThreshold > 0 && in.QueueWaitMax >= cfg.QueueWaitMaxThreshold {
		triggers = append(triggers, trigger{ScaleReasonQueueWaitHigh, true, 6})
	}
	if depthRatio >= cfg.QueueDepthRatioThreshold {
		triggers = append(triggers, trigger{ScaleReasonQueueDepthHigh, true, 7})
	}

	best := ScaleReasonNone
	bestScope := ScaleScopeNone
	unhealthy := false
	bestPri := 999
	for _, t := range triggers {
		if !t.fired {
			continue
		}
		unhealthy = true
		if t.priority < bestPri {
			bestPri = t.priority
			best = t.reason
			bestScope = scopeForScaleTrigger(t.reason, in)
		}
	}
	if !unhealthy {
		return ScaleReasonNone, ScaleScopeNone, false
	}
	return best, bestScope, true
}

func (s *Scheduler) admissionCounterSample() admissionCounterSample {
	var out admissionCounterSample
	for i := range s.laneCounters {
		submitted, rejected, shed := s.laneCounters[i].admissionSampleFragment()
		out.submitted += submitted
		out.rejected += rejected
		out.shed += shed
	}
	out.throttled = s.perKeyThrottledTotal.Load()
	return out
}

func (s *Scheduler) schedulerQueueWaitMax() time.Duration {
	var maxNanos uint64
	for i := range s.laneCounters {
		if n := s.laneCounters[i].queueWaitMaxNanos(); n > maxNanos {
			maxNanos = n
		}
	}
	return time.Duration(maxNanos)
}

func admissionRates(current, last admissionCounterSample) (rejectRate, shedRate, throttleRate float64) {
	deltaSubmitted := current.submitted - last.submitted
	if deltaSubmitted == 0 {
		if current.submitted > 0 && last.submitted == 0 {
			deltaSubmitted = current.submitted
		} else {
			return 0, 0, 0
		}
	}
	deltaReject := current.rejected - last.rejected
	deltaShed := current.shed - last.shed
	deltaThrottle := current.throttled - last.throttled
	rejectRate = float64(deltaReject) / float64(deltaSubmitted)
	shedRate = float64(deltaShed) / float64(deltaSubmitted)
	throttleRate = float64(deltaThrottle) / float64(deltaSubmitted)
	if rejectRate > 1 {
		rejectRate = 1
	}
	if shedRate > 1 {
		shedRate = 1
	}
	if throttleRate > 1 {
		throttleRate = 1
	}
	return rejectRate, shedRate, throttleRate
}

func (s *Scheduler) buildScaleSignalShardAggregates(view schedulerDebugView) scaleShardAggregates {
	spCfg := s.shardPressureCfg
	normalizeShardPressureConfig(&spCfg)

	shardCount := len(view.shards)
	if shardCount == 0 {
		return scaleShardAggregates{pressureClass: ShardPressureUnknown}
	}

	windowNanos := uint64(spCfg.Window.Nanoseconds())
	ratios := make([]float64, shardCount)
	var peerSum float64
	for i, sh := range view.shards {
		depthRatio := safeRatio(sh.depth, sh.capacity)
		waitRatio := computeQueueWaitRatio(sh.queueWaitNanos, windowNanos, view.workerCount)
		workerRatio := computeWorkerContributionRatio(int64(sh.inFlight), view.workerCount)
		ratio := computeShardPressureRatio(depthRatio, waitRatio, 0, workerRatio)
		ratios[i] = ratio
		peerSum += ratio
	}

	var hotCount, localizedShardCount, hotKeyCount int
	var maxSkew, maxLocalized float64
	hotLimit := spCfg.MaxHotShards
	if hotLimit <= 0 {
		hotLimit = 16
	}
	hotShardIDs := make([]int, 0, hotLimit)

	for i, sh := range view.shards {
		ratio := ratios[i]
		isHot := false
		if shardPressureEnabled(spCfg) {
			isHot = ratio >= spCfg.HotShardPressureRatio
		} else {
			isHot = sh.depth > 0 || sh.inFlight > 0
		}
		if isHot {
			hotCount++
			if len(hotShardIDs) < hotLimit {
				hotShardIDs = append(hotShardIDs, i)
			}
		}

		peerCount := shardCount - 1
		if peerCount > 0 {
			peerOnly := peerSum - ratio
			skew := ratio / (peerOnly / float64(peerCount))
			if skew > maxSkew {
				maxSkew = skew
			}
		}
	}

	for _, shardID := range hotShardIDs {
		if shardID >= len(view.shards) {
			continue
		}
		sh := view.shards[shardID]
		if shardID >= len(s.hotKeyTrackers) || s.hotKeyTrackers[shardID] == nil || !s.hotKeyTrackers[shardID].enabled() {
			continue
		}
		_, cands := s.hotKeyTrackers[shardID].detectHotKeyCandidates(shardID, sh.depth, sh.queueWaitNanos)
		hotKeyCount += len(cands)
		shardMax := 0.0
		for _, c := range cands {
			if c.DepthRatio > shardMax {
				shardMax = c.DepthRatio
			}
			if c.DepthRatio > maxLocalized {
				maxLocalized = c.DepthRatio
			}
		}
		locThreshold := spCfg.LocalizedHotKeyRatio
		if locThreshold <= 0 {
			locThreshold = 0.40
		}
		if shardMax >= locThreshold {
			localizedShardCount++
		}
	}

	if !shardPressureEnabled(spCfg) {
		hot := rankHotShards(view.shards, hotLimit)
		hotCount = len(hot)
	}

	hotShardRatio := 0.0
	if shardCount > 0 {
		hotShardRatio = float64(hotCount) / float64(shardCount)
	}

	pressure := s.Pressure()
	queueDepthRatio := pressure.TotalDepthRatio
	queueWaitRatio := 0.0
	if windowNanos > 0 && view.workerCount > 0 {
		var totalWait uint64
		for _, sh := range view.shards {
			totalWait += sh.queueWaitNanos
		}
		queueWaitRatio = computeQueueWaitRatio(totalWait, windowNanos, view.workerCount)
	}
	workerBusy := 0.0
	if view.workerCount > 0 {
		workerBusy = float64(pressure.TotalInFlight) / float64(view.workerCount)
		if workerBusy > 1 {
			workerBusy = 1
		}
	}

	class := ShardPressureHealthy
	if shardPressureEnabled(spCfg) {
		class = classifyGlobalPressure(globalPressureInput{
			ShardCount:          shardCount,
			HotShardCount:       hotCount,
			HotShardRatio:       hotShardRatio,
			QueueDepthRatio:     queueDepthRatio,
			QueueWaitRatio:      queueWaitRatio,
			WorkerBusyRatio:     workerBusy,
			MaxSkewRatio:        maxSkew,
			LocalizedShardCount: localizedShardCount,
			Cfg:                 spCfg,
		})
	}

	return scaleShardAggregates{
		hotShardCount:        hotCount,
		hotShardRatio:        hotShardRatio,
		hotKeyCandidateCount: hotKeyCount,
		localizedRatio:       maxLocalized,
		pressureClass:        class,
	}
}

func (s *Scheduler) buildScaleSignalRawInput() scaleSignalRawInput {
	pressure := s.Pressure()
	view := s.collectSchedulerDebugView()
	aggregates := s.buildScaleSignalShardAggregates(view)
	counters := s.admissionCounterSample()

	workerBusy := 0.0
	if s.workerCount > 0 {
		workerBusy = float64(pressure.TotalInFlight) / float64(s.workerCount)
		if workerBusy > 1 {
			workerBusy = 1
		}
	}

	in := scaleSignalInput{
		QueueDepthTotal:      int64(pressure.TotalDepth),
		QueueCapacityTotal:   int64(pressure.TotalCapacity),
		QueueWaitMax:         s.schedulerQueueWaitMax(),
		WorkerBusyRatio:      workerBusy,
		HotShardCount:        aggregates.hotShardCount,
		TotalShardCount:      len(view.shards),
		HotShardRatio:        aggregates.hotShardRatio,
		HotKeyCandidateCount: aggregates.hotKeyCandidateCount,
		LocalizedHotKeyRatio: aggregates.localizedRatio,
		ShardPressureClass:   aggregates.pressureClass,
	}

	return scaleSignalRawInput{
		scaleSignalInput: in,
		counters:         counters,
	}
}

// ConfigureAutoscalingSignal applies autoscaling signal configuration.
func (s *Scheduler) ConfigureAutoscalingSignal(cfg AutoscalingSignalConfig) {
	normalizeAutoscalingSignalConfig(&cfg)
	calc := s.scaleCalc
	if calc == nil {
		calc = &scaleSignalCalculator{cfg: cfg}
		s.scaleCalc = calc
		return
	}
	calc.mu.Lock()
	calc.cfg = cfg
	calc.unhealthyWindows = 0
	calc.lastSampleAt = time.Time{}
	calc.lastCounters = admissionCounterSample{}
	calc.mu.Unlock()
}

// ScaleSignalSnapshot returns the latest autoscaling signal (KL-1504).
func (s *Scheduler) ScaleSignalSnapshot() ScaleSignal {
	now := time.Now()
	calc := s.scaleCalc
	if calc == nil {
		return ScaleSignal{
			DiagnosticsEnabled: false,
			Reason:             ScaleReasonNone,
			Scope:              ScaleScopeNone,
			WindowEndedAt:      now,
		}
	}

	calc.mu.Lock()
	cfg := calc.cfg
	if !autoscalingEnabled(cfg) {
		calc.mu.Unlock()
		return ScaleSignal{
			DiagnosticsEnabled: false,
			Reason:             ScaleReasonNone,
			Scope:              ScaleScopeNone,
			WindowEndedAt:      now,
		}
	}
	calc.mu.Unlock()

	raw := s.buildScaleSignalRawInput()

	calc.mu.Lock()
	defer calc.mu.Unlock()

	rejectRate, shedRate, throttleRate := admissionRates(raw.counters, calc.lastCounters)
	if calc.lastSampleAt.IsZero() && raw.counters.submitted > 0 {
		rejectRate = float64(raw.counters.rejected) / float64(raw.counters.submitted)
		shedRate = float64(raw.counters.shed) / float64(raw.counters.submitted)
		throttleRate = float64(raw.counters.throttled) / float64(raw.counters.submitted)
		if rejectRate > 1 {
			rejectRate = 1
		}
		if shedRate > 1 {
			shedRate = 1
		}
		if throttleRate > 1 {
			throttleRate = 1
		}
	}

	in := raw.scaleSignalInput
	in.AdmissionRejectedRate = rejectRate
	in.AdmissionShedRate = shedRate
	in.AdmissionThrottledRate = throttleRate

	return calc.calculate(in, raw.counters, now)
}

// PerKeyThrottledTotal returns cumulative per-key throttle decisions since queue start.
func (s *Scheduler) PerKeyThrottledTotal() uint64 {
	return s.perKeyThrottledTotal.Load()
}

// ScaleAdmissionTotals returns cumulative admission counters aggregated across lanes.
func (s *Scheduler) ScaleAdmissionTotals() (rejected, shed, throttled uint64) {
	sample := s.admissionCounterSample()
	return sample.rejected, sample.shed, sample.throttled
}
