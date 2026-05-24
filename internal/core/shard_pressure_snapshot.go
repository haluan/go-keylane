// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"sort"
	"time"
)

type perKeyMitigationLookup map[uint64]PerKeyAdmissionSnapshot

type pressureViewBundle struct {
	view        schedulerDebugView
	perKeySnaps []PerKeyAdmissionSnapshot
	summaries   []ShardPressureSnapshot
}

func buildPerKeyMitigationLookup(source []PerKeyAdmissionSnapshot, shardID int) perKeyMitigationLookup {
	out := make(perKeyMitigationLookup)
	for _, s := range source {
		if s.ShardID != shardID {
			continue
		}
		out[s.KeyHash] = s
	}
	return out
}

func (t *hotKeyTracker) entryMitigation(keyHash uint64) (PerKeyMitigationAction, PerKeyAdmissionReason) {
	if t == nil || !t.enabled() {
		return "", ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	idx, ok := t.index[keyHash]
	if !ok {
		return "", ""
	}
	e := &t.entries[idx]
	if e.keyHash != keyHash {
		return "", ""
	}
	return e.lastAction, e.lastReason
}

func hotKeyToPressureSnapshot(c HotKeyCandidate, shardDepth uint64, shardWait uint64, mit perKeyMitigationLookup, fallbackAction PerKeyMitigationAction, fallbackReason PerKeyAdmissionReason) HotKeyPressureSnapshot {
	var depthContrib, waitContrib, admitContrib float64
	if shardDepth > 0 && c.QueuedApprox > 0 {
		depthContrib = float64(c.QueuedApprox) / float64(shardDepth)
	}
	if shardWait > 0 && c.WaitRatio > 0 {
		waitContrib = c.WaitRatio
	}
	if c.SubmittedApprox > 0 {
		admitContrib = float64(c.RejectedApprox) / float64(c.SubmittedApprox)
	}

	snap := HotKeyPressureSnapshot{
		KeyHash:                    c.KeyHash,
		LaneID:                     c.LaneID,
		Status:                     c.Status,
		QueuedApprox:               c.QueuedApprox,
		SubmittedApprox:            c.SubmittedApprox,
		DepthContributionRatio:     depthContrib,
		WaitContributionRatio:      waitContrib,
		AdmissionContributionRatio: admitContrib,
		LastSeen:                   c.LastSeen,
	}

	action := fallbackAction
	reason := fallbackReason
	if pk, ok := mit[c.KeyHash]; ok {
		action = pk.Action
		reason = pk.Reason
	}
	count := c.RejectedApprox
	switch action {
	case PerKeyMitigationThrottle:
		snap.ThrottledApprox = count
	case PerKeyMitigationReject:
		snap.RejectedApprox = count
	case PerKeyMitigationShed:
		snap.ShedApprox = count
	}
	if action != "" && action != PerKeyMitigationAllow {
		snap.ActiveMitigation = action
		snap.MitigationReason = reason
	}
	return snap
}

func perLaneThrottleCounts(hotKeys []HotKeyPressureSnapshot) map[uint16]uint64 {
	out := make(map[uint16]uint64)
	for _, hk := range hotKeys {
		if hk.ActiveMitigation == PerKeyMitigationThrottle {
			out[hk.LaneID] += hk.ThrottledApprox
		}
	}
	return out
}

func buildLaneBreakdown(
	sh shardDebugView,
	lanes []laneDebugView,
	laneReg *LaneRegistry,
	cfg ShardPressureConfig,
	windowNanos uint64,
	workerCount int,
	throttleByLane map[uint16]uint64,
) ([]LanePressureSnapshot, *LanePressureSnapshot, float64) {
	if sh.depth == 0 || len(sh.laneDeps) == 0 {
		return nil, nil, 0
	}
	limit := cfg.MaxLaneBreakdownPerShard
	if limit <= 0 {
		limit = 8
	}
	var breakdown []LanePressureSnapshot
	var topContrib float64
	var dominant *LanePressureSnapshot
	for _, ld := range sh.laneDeps {
		if ld.depth == 0 {
			continue
		}
		laneIdx := int(ld.laneID)
		if laneIdx < 0 || laneIdx >= len(lanes) {
			continue
		}
		ln := lanes[laneIdx]
		contrib := float64(ld.depth) / float64(sh.depth)
		if contrib > topContrib {
			topContrib = contrib
		}
		share := laneDepthShare(ld.depth, ln.depth)
		depthRatio := safeRatio(ld.depth, sh.capacity)
		waitNanos := uint64(float64(ln.queueWaitTotal) * share)
		waitRatio := computeQueueWaitRatio(waitNanos, windowNanos, workerCount)
		inflight := int64(float64(ln.inFlight) * share)

		reject := uint64(float64(ln.admissionRejected+ln.overloadRejected) * share)
		shed := uint64(float64(ln.overloadShed) * share)
		completed := uint64(float64(ln.completed) * share)
		throttled := uint64(0)
		if throttleByLane != nil {
			throttled = throttleByLane[uint16(ld.laneID)]
		}

		lp := LanePressureSnapshot{
			LaneID:               uint16(ld.laneID),
			Name:                 laneReg.Name(ld.laneID),
			QueueDepth:           int64(ld.depth),
			QueueDepthRatio:      depthRatio,
			QueueWaitApproxNanos: waitNanos,
			QueueWaitRatio:       waitRatio,
			InflightJobs:         inflight,
			CompletedApprox:      completed,
			RejectedApprox:       reject,
			ThrottledApprox:      throttled,
			ShedApprox:           shed,
			PressureRatio:        depthRatio,
			ContributionRatio:    contrib,
		}
		breakdown = append(breakdown, lp)
	}
	sort.Slice(breakdown, func(i, j int) bool {
		if breakdown[i].ContributionRatio != breakdown[j].ContributionRatio {
			return breakdown[i].ContributionRatio > breakdown[j].ContributionRatio
		}
		return breakdown[i].LaneID < breakdown[j].LaneID
	})
	if len(breakdown) > limit {
		breakdown = breakdown[:limit]
	}
	if len(breakdown) > 0 && breakdown[0].ContributionRatio >= cfg.DominantLaneRatio {
		d := breakdown[0]
		dominant = &d
	}
	return breakdown, dominant, topContrib
}

func (s *Scheduler) buildShardPressureSnapshot(
	shardID int,
	view schedulerDebugView,
	cfg ShardPressureConfig,
	perKeySnaps []PerKeyAdmissionSnapshot,
	peerPressureSum float64,
	peerCount int,
	now time.Time,
) ShardPressureSnapshot {
	if shardID < 0 || shardID >= len(view.shards) {
		return ShardPressureSnapshot{ShardID: shardID, Class: ShardPressureUnknown, UpdatedAt: now}
	}
	sh := view.shards[shardID]
	windowNanos := uint64(cfg.Window.Nanoseconds())

	depthRatio := safeRatio(sh.depth, sh.capacity)
	waitRatio := computeQueueWaitRatio(sh.queueWaitNanos, windowNanos, view.workerCount)
	workerRatio := computeWorkerContributionRatio(int64(sh.inFlight), view.workerCount)

	var hotKeySnaps []HotKeyPressureSnapshot
	var topKeyContrib float64
	var hasHotKey bool

	mitLookup := buildPerKeyMitigationLookup(perKeySnaps, shardID)
	if shardID < len(s.hotKeyTrackers) && s.hotKeyTrackers[shardID] != nil && s.hotKeyTrackers[shardID].enabled() {
		hk := s.hotKeyTrackers[shardID]
		_, candidates := hk.detectHotKeyCandidates(shardID, sh.depth, sh.queueWaitNanos)
		limit := cfg.MaxHotKeyCandidatesPerShard
		if limit <= 0 {
			limit = 4
		}
		if len(candidates) > limit {
			candidates = candidates[:limit]
		}
		for _, c := range candidates {
			c.Key = ""
			action, reason := hk.entryMitigation(c.KeyHash)
			snap := hotKeyToPressureSnapshot(c, sh.depth, sh.queueWaitNanos, mitLookup, action, reason)
			hotKeySnaps = append(hotKeySnaps, snap)
			hasHotKey = true
			contrib := maxFloat64(snap.DepthContributionRatio, snap.WaitContributionRatio, snap.AdmissionContributionRatio)
			if contrib > topKeyContrib {
				topKeyContrib = contrib
			}
		}
	}

	throttleByLane := perLaneThrottleCounts(hotKeySnaps)
	admitTotals := shardLaneAdmissionTotals(sh, view.lanes)
	// Per-key throttles are added here because lane counters track rejects/sheds only;
	// per-key admission errors short-circuit before recordLaneAdmissionResult. If lane
	// counters ever gain a throttle counter, remove this add to avoid double-count.
	for _, hk := range hotKeySnaps {
		admitTotals.throttled += hk.ThrottledApprox
	}

	admissionRatio := computeAdmissionPressureRatio(
		admitTotals.rejected, admitTotals.throttled, admitTotals.shed, admitTotals.submitted,
	)
	pressureRatio := computeShardPressureRatio(depthRatio, waitRatio, admissionRatio, workerRatio)
	peerAvg, skew := computePeerPressureRatio(pressureRatio, peerPressureSum, peerCount)

	lanes, dominant, topLaneContrib := buildLaneBreakdown(
		sh, view.lanes, s.laneReg, cfg, windowNanos, view.workerCount, throttleByLane,
	)

	classInput := shardPressureInput{
		ShardID:                 shardID,
		QueueDepth:              int64(sh.depth),
		QueueCapacity:           int64(sh.capacity),
		QueueDepthRatio:         depthRatio,
		QueueWaitApproxNanos:    sh.queueWaitNanos,
		QueueWaitRatio:          waitRatio,
		InflightJobs:            int64(sh.inFlight),
		AdmissionPressureRatio:  admissionRatio,
		WorkerContributionRatio: workerRatio,
		PressureRatio:           pressureRatio,
		PeerPressureRatio:       peerAvg,
		SkewRatio:               skew,
		TopHotKeyContribution:   topKeyContrib,
		TopLaneContribution:     topLaneContrib,
		HasHotKeyCandidate:      hasHotKey,
		Cfg:                     cfg,
	}

	return ShardPressureSnapshot{
		ShardID:              shardID,
		DiagnosticsEnabled:   true,
		Class:                classifyShardPressure(classInput),
		QueueDepth:           int64(sh.depth),
		QueueCapacity:        int64(sh.capacity),
		QueueDepthRatio:      depthRatio,
		QueueWaitApproxNanos: sh.queueWaitNanos,
		QueueWaitRatio:       waitRatio,
		InflightJobs:         int64(sh.inFlight),
		CompletedApprox:      admitTotals.completed,
		RejectedApprox:       admitTotals.rejected,
		ThrottledApprox:      admitTotals.throttled,
		ShedApprox:           admitTotals.shed,
		PressureRatio:        pressureRatio,
		PeerPressureRatio:    peerAvg,
		SkewRatio:            skew,
		DominantLane:         dominant,
		LaneBreakdown:        lanes,
		HotKeyCandidates:     hotKeySnaps,
		UpdatedAt:            now,
	}
}

func rankHotShardPressures(summaries []ShardPressureSnapshot, limit int) []ShardPressureSnapshot {
	if limit <= 0 || len(summaries) == 0 {
		return nil
	}
	var hot []ShardPressureSnapshot
	for _, sh := range summaries {
		if sh.PressureRatio <= 0 && sh.QueueDepth <= 0 {
			continue
		}
		hot = append(hot, sh)
	}
	sort.Slice(hot, func(i, j int) bool {
		a, b := hot[i], hot[j]
		if a.PressureRatio != b.PressureRatio {
			return a.PressureRatio > b.PressureRatio
		}
		if a.QueueDepth != b.QueueDepth {
			return a.QueueDepth > b.QueueDepth
		}
		return a.ShardID < b.ShardID
	})
	if len(hot) > limit {
		hot = hot[:limit]
	}
	return hot
}

func hotShardPressuresFromBundle(bundle pressureViewBundle, cfg ShardPressureConfig) []ShardPressureSnapshot {
	hotLimit := cfg.MaxHotShards
	if hotLimit <= 0 {
		hotLimit = 16
	}
	return rankHotShardPressures(bundle.summaries, hotLimit)
}

func (s *Scheduler) collectPressureView(cfg ShardPressureConfig, now time.Time) pressureViewBundle {
	view := s.collectSchedulerDebugView()
	perKeySnaps := s.PerKeyAdmissionSnapshots()
	shardCount := len(view.shards)
	if shardCount == 0 {
		return pressureViewBundle{view: view, perKeySnaps: perKeySnaps}
	}

	type partial struct {
		ratio float64
	}
	partials := make([]partial, shardCount)
	var peerSum float64
	windowNanos := uint64(cfg.Window.Nanoseconds())
	for i := 0; i < shardCount; i++ {
		sh := view.shards[i]
		depthRatio := safeRatio(sh.depth, sh.capacity)
		waitRatio := computeQueueWaitRatio(sh.queueWaitNanos, windowNanos, view.workerCount)
		workerRatio := computeWorkerContributionRatio(int64(sh.inFlight), view.workerCount)
		ratio := computeShardPressureRatio(depthRatio, waitRatio, 0, workerRatio)
		partials[i].ratio = ratio
		peerSum += ratio
	}

	summaries := make([]ShardPressureSnapshot, shardCount)
	for i := 0; i < shardCount; i++ {
		peerCount := shardCount - 1
		peerOnlySum := peerSum - partials[i].ratio
		summaries[i] = s.buildShardPressureSnapshot(i, view, cfg, perKeySnaps, peerOnlySum, peerCount, now)
	}
	return pressureViewBundle{view: view, perKeySnaps: perKeySnaps, summaries: summaries}
}

func (s *Scheduler) buildPressureSummary(bundle pressureViewBundle, cfg ShardPressureConfig, now time.Time) PressureSummarySnapshot {
	if !shardPressureEnabled(cfg) {
		return PressureSummarySnapshot{DiagnosticsEnabled: false, UpdatedAt: now}
	}

	summaries := bundle.summaries
	shardCount := len(summaries)
	if shardCount == 0 {
		return PressureSummarySnapshot{
			DiagnosticsEnabled: true,
			Class:              ShardPressureUnknown,
			UpdatedAt:          now,
		}
	}

	totalDepth, totalCapacity, totalInFlight := debugViewTotals(bundle.view)
	var totalWait uint64
	for _, sh := range bundle.view.shards {
		totalWait += sh.queueWaitNanos
	}
	globalDepthRatio := safeRatio(totalDepth, totalCapacity)
	windowNanos := uint64(cfg.Window.Nanoseconds())
	globalWaitRatio := computeQueueWaitRatio(totalWait, windowNanos, bundle.view.workerCount)
	workerBusy := computeWorkerContributionRatio(int64(totalInFlight), bundle.view.workerCount)

	hotLimit := cfg.MaxHotShards
	if hotLimit <= 0 {
		hotLimit = 16
	}
	hotShards := rankHotShardPressures(summaries, hotLimit)

	hotCount := 0
	var localizedCount int
	var maxSkew float64
	var localizedRatio, laneDomRatio float64
	for _, sh := range summaries {
		if sh.PressureRatio >= cfg.HotShardPressureRatio {
			hotCount++
		}
		if sh.Class == ShardPressureLocalizedKey {
			localizedCount++
			if sh.PressureRatio > localizedRatio {
				localizedRatio = sh.PressureRatio
			}
		}
		if sh.DominantLane != nil && sh.DominantLane.ContributionRatio > laneDomRatio {
			laneDomRatio = sh.DominantLane.ContributionRatio
		}
		if sh.SkewRatio > maxSkew {
			maxSkew = sh.SkewRatio
		}
	}

	hotShardRatio := 0.0
	if shardCount > 0 {
		hotShardRatio = float64(hotCount) / float64(shardCount)
	}

	globalInput := globalPressureInput{
		ShardCount:          shardCount,
		HotShardCount:       hotCount,
		HotShardRatio:       hotShardRatio,
		QueueDepthRatio:     globalDepthRatio,
		QueueWaitRatio:      globalWaitRatio,
		WorkerBusyRatio:     workerBusy,
		MaxSkewRatio:        maxSkew,
		LocalizedShardCount: localizedCount,
		Cfg:                 cfg,
	}
	class := classifyGlobalPressure(globalInput)
	scale, mitigation := computeScaleMitigationFlags(class, globalInput)

	return PressureSummarySnapshot{
		DiagnosticsEnabled:       true,
		Class:                    class,
		TotalQueueDepth:          int64(totalDepth),
		TotalQueueCapacity:       int64(totalCapacity),
		QueueDepthRatio:          globalDepthRatio,
		TotalQueueWaitNanos:      totalWait,
		HotShardCount:            hotCount,
		HotShardRatio:            hotShardRatio,
		WorkerBusyRatio:          workerBusy,
		InflightJobs:             int64(totalInFlight),
		DistributedPressureRatio: hotShardRatio,
		LocalizedPressureRatio:   localizedRatio,
		LaneDominanceRatio:       laneDomRatio,
		ScaleRelevant:            scale,
		MitigationRelevant:       mitigation,
		HotShards:                hotShards,
		UpdatedAt:                now,
	}
}

func (s *Scheduler) PressureSummarySnapshot() PressureSummarySnapshot {
	cfg := s.shardPressureCfg
	normalizeShardPressureConfig(&cfg)
	if !shardPressureEnabled(cfg) {
		return PressureSummarySnapshot{DiagnosticsEnabled: false, UpdatedAt: time.Now()}
	}
	bundle := s.collectPressureView(cfg, time.Now())
	return s.buildPressureSummary(bundle, cfg, time.Now())
}

func (s *Scheduler) ShardPressureSnapshot(shardID int) (ShardPressureSnapshot, bool) {
	cfg := s.shardPressureCfg
	normalizeShardPressureConfig(&cfg)
	if shardID < 0 || shardID >= len(s.shards) {
		return ShardPressureSnapshot{}, false
	}
	if !shardPressureEnabled(cfg) {
		return ShardPressureSnapshot{
			DiagnosticsEnabled: false,
			ShardID:            shardID,
			UpdatedAt:          time.Now(),
		}, true
	}
	bundle := s.collectPressureView(cfg, time.Now())
	if shardID >= len(bundle.summaries) {
		return ShardPressureSnapshot{}, false
	}
	return bundle.summaries[shardID], true
}

func (s *Scheduler) HotShardPressureSnapshots() []ShardPressureSnapshot {
	cfg := s.shardPressureCfg
	normalizeShardPressureConfig(&cfg)
	if !shardPressureEnabled(cfg) {
		return nil
	}
	bundle := s.collectPressureView(cfg, time.Now())
	hot := hotShardPressuresFromBundle(bundle, cfg)
	if len(hot) == 0 {
		return nil
	}
	out := make([]ShardPressureSnapshot, len(hot))
	copy(out, hot)
	return out
}

func (s *Scheduler) AppendHotShardPressureSnapshots(dst []ShardPressureSnapshot) []ShardPressureSnapshot {
	cfg := s.shardPressureCfg
	normalizeShardPressureConfig(&cfg)
	if !shardPressureEnabled(cfg) {
		return dst
	}
	bundle := s.collectPressureView(cfg, time.Now())
	hot := hotShardPressuresFromBundle(bundle, cfg)
	if len(hot) == 0 {
		return dst
	}
	return append(dst, hot...)
}
