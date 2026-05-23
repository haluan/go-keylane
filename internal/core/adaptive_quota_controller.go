// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// LaneQuotaApplier applies a single-lane quota change through the safe update path.
type LaneQuotaApplier func(ctx context.Context, lane string, quota uint32, expectedQuotaVersion uint64) (uint64, error)

// AdaptiveQuotaDecisionHook is invoked after a quota adjustment attempt (success or apply failure).
type AdaptiveQuotaDecisionHook func(QuotaAdjustmentDecision, time.Time)

// AdaptiveQuotaController runs periodic adaptive quota evaluation.
type AdaptiveQuotaController struct {
	cfg           AdaptiveQuotaConfig
	sched         *Scheduler
	reg           *LaneRegistry
	policies      []resolvedLaneAdaptivePolicy
	policyVersion uint64
	apply         LaneQuotaApplier
	onDecision    AdaptiveQuotaDecisionHook

	mu        sync.Mutex
	state     *AdaptiveControllerState
	running   atomic.Bool
	lastEval  time.Time
	tickCount atomic.Uint64
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewAdaptiveQuotaController creates a controller; cfg.Enabled may be false.
func NewAdaptiveQuotaController(
	sched *Scheduler,
	reg *LaneRegistry,
	cfg AdaptiveQuotaConfig,
	explicit []LaneAdaptivePolicy,
	initialQuotas map[string]int,
	apply LaneQuotaApplier,
	onDecision AdaptiveQuotaDecisionHook,
) *AdaptiveQuotaController {
	return &AdaptiveQuotaController{
		cfg:           cfg,
		sched:         sched,
		reg:           reg,
		policies:      resolveAdaptiveLanePolicies(reg, sched, explicit, initialQuotas),
		policyVersion: 1,
		apply:         apply,
		onDecision:    onDecision,
	}
}

// Start begins the evaluation loop. Idempotent when already running.
func (c *AdaptiveQuotaController) Start(ctx context.Context) error {
	if !c.cfg.Enabled {
		return nil
	}
	c.mu.Lock()
	if c.running.Load() {
		c.mu.Unlock()
		return nil
	}
	if c.state == nil {
		c.state = newAdaptiveControllerState(time.Now(), c.reg.Len())
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.mu.Unlock()

	c.running.Store(true)
	c.wg.Add(1)
	go c.loop(runCtx)
	return nil
}

// Stop stops the evaluation loop. Idempotent.
func (c *AdaptiveQuotaController) Stop() {
	c.mu.Lock()
	cancel := c.cancel
	c.cancel = nil
	c.mu.Unlock()

	if cancel == nil && !c.running.Load() {
		return
	}
	if cancel != nil {
		cancel()
	}
	c.wg.Wait()
	c.running.Store(false)
}

func (c *AdaptiveQuotaController) loop(ctx context.Context) {
	defer c.wg.Done()
	defer c.running.Store(false)

	ticker := time.NewTicker(c.cfg.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

// RunTick performs one evaluation cycle. It is intended for tests.
func (c *AdaptiveQuotaController) RunTick() {
	c.RunTickContext(context.Background())
}

// RunTickContext performs one evaluation cycle with the given context.
func (c *AdaptiveQuotaController) RunTickContext(ctx context.Context) {
	c.tick(ctx)
}

// DebugSignalSnapshot builds the signal snapshot used by the evaluator (for tests).
func (c *AdaptiveQuotaController) DebugSignalSnapshot() AdaptiveSignalSnapshot {
	return buildAdaptiveSignalSnapshot(c.sched, c.reg, c.policies, c.policyVersion)
}

// DebugConfig returns the controller config (for tests).
func (c *AdaptiveQuotaController) DebugConfig() AdaptiveQuotaConfig {
	return c.cfg
}

// DebugPolicyForLane returns the resolved policy for a lane name (for tests).
func (c *AdaptiveQuotaController) DebugPolicyForLane(lane string) (resolvedLaneAdaptivePolicy, bool) {
	for _, p := range c.policies {
		if p.Lane == lane {
			return p, true
		}
	}
	return resolvedLaneAdaptivePolicy{}, false
}

// DebugEvaluate runs the evaluator without applying changes (for tests).
func (c *AdaptiveQuotaController) DebugEvaluate(now time.Time) []QuotaAdjustmentDecision {
	c.mu.Lock()
	state := c.state
	if state == nil {
		state = newAdaptiveControllerState(now, c.reg.Len())
		c.state = state
	}
	c.mu.Unlock()
	snap := buildAdaptiveSignalSnapshot(c.sched, c.reg, c.policies, c.policyVersion)
	return EvaluateAdaptiveQuota(c.cfg, c.policies, snap, state, now)
}

func (c *AdaptiveQuotaController) tick(ctx context.Context) {
	if c.apply == nil {
		return
	}
	now := time.Now()
	c.mu.Lock()
	state := c.state
	if state == nil {
		state = newAdaptiveControllerState(now, c.reg.Len())
		c.state = state
	}
	c.mu.Unlock()

	snap := buildAdaptiveSignalSnapshot(c.sched, c.reg, c.policies, c.policyVersion)
	decisions := EvaluateAdaptiveQuota(c.cfg, c.policies, snap, state, now)

	c.mu.Lock()
	state.TickCount++
	c.tickCount.Add(1)
	c.lastEval = now
	c.mu.Unlock()

	for _, d := range decisions {
		if d.Action == QuotaAdjustmentHold || d.NewQuota == d.OldQuota {
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
		}

		ver, err := c.apply(ctx, d.Lane, uint32(d.NewQuota), snap.QuotaVersion)
		if err != nil {
			failed := d
			failed.Action = QuotaAdjustmentHold
			failed.Reason = QuotaReasonUpdateFailed
			failed.NewQuota = failed.OldQuota
			failed.PolicyVersion = snap.PolicyVersion
			failed.QuotaVersion = snap.QuotaVersion
			c.mu.Lock()
			state.appendDecision(failed)
			c.mu.Unlock()
			if c.onDecision != nil {
				c.onDecision(failed, now)
			}
			continue
		}
		d.QuotaVersion = ver
		d.PolicyVersion = snap.PolicyVersion
		c.mu.Lock()
		if id, ok := c.reg.Lookup(d.Lane); ok {
			state.recordApplied(id, now)
		}
		state.appendDecision(d)
		c.mu.Unlock()
		if c.onDecision != nil {
			c.onDecision(d, now)
		}
	}
}

// Snapshot returns a copy of controller state for diagnostics.
func (c *AdaptiveQuotaController) Snapshot() (enabled, running bool, lastEval time.Time, tickCount uint64, decisions []QuotaAdjustmentDecision, policyVer, quotaVer uint64) {
	enabled = c.cfg.Enabled
	running = c.running.Load()
	c.mu.Lock()
	lastEval = c.lastEval
	if c.state != nil {
		decisions = append([]QuotaAdjustmentDecision(nil), c.state.LastDecisions...)
	}
	c.mu.Unlock()
	tickCount = c.tickCount.Load()
	policyVer = c.policyVersion
	quotaVer, _, _ = c.sched.CurrentQuotaPolicyView()
	return enabled, running, lastEval, tickCount, decisions, policyVer, quotaVer
}
