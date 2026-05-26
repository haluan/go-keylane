// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"sync"
	"sync/atomic"
	"time"
)

// ContinuationSnapshot is a point-in-time copy of continuation registry diagnostics.
type ContinuationSnapshot struct {
	Pending         int
	MaxPending      int
	Completed       uint64
	Failed          uint64
	Cancelled       uint64
	LateCompletions uint64
	ResumeRejected  uint64
}

// ShardContinuationSnapshot summarises pending continuations for one shard.
type ShardContinuationSnapshot struct {
	ShardID    int
	Pending    int
	MaxPending int
}

// pendingEntry is the registry's in-flight record for one continuation.
// The generic state is erased; completion signals are routed through doneCh.
type pendingEntry struct {
	id           ContinuationID
	shardID      int
	registeredAt time.Time
	// closeDone closes the continuation's done channel exactly once on resolution.
	closeDone func()
}

// continuationRegistry is the bounded in-memory store for pending continuations.
// It is owned by Queue and initialised only when ContinuationConfig.Enabled is true.
type continuationRegistry struct {
	cfg ContinuationConfig

	mu      sync.Mutex
	pending map[ContinuationID]pendingEntry
	nextID  atomic.Uint64

	completed       atomic.Uint64
	failed          atomic.Uint64
	cancelled       atomic.Uint64
	lateCompletions atomic.Uint64
	resumeRejected  atomic.Uint64

	// perShard tracks pending count per shard when MaxPendingPerShard > 0.
	perShard map[int]int
}

func newContinuationRegistry(cfg ContinuationConfig) *continuationRegistry {
	return &continuationRegistry{
		cfg:      cfg,
		pending:  make(map[ContinuationID]pendingEntry),
		perShard: make(map[int]int),
	}
}

// allocID returns a new unique ContinuationID (never zero).
func (r *continuationRegistry) allocID() ContinuationID {
	id := r.nextID.Add(1)
	return ContinuationID(id)
}

// register adds a pending continuation to the registry.
// Returns ErrContinuationLimitExceeded when capacity is exhausted.
func (r *continuationRegistry) register(entry pendingEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cfg.MaxPending > 0 && len(r.pending) >= r.cfg.MaxPending {
		return ErrContinuationLimitExceeded
	}
	if r.cfg.MaxPendingPerShard > 0 && r.perShard[entry.shardID] >= r.cfg.MaxPendingPerShard {
		return ErrContinuationLimitExceeded
	}

	r.pending[entry.id] = entry
	r.perShard[entry.shardID]++
	return nil
}

// resolveOutcome represents the registry's view of how a continuation resolved.
type resolveOutcome int

const (
	resolveOutcomeCompleted resolveOutcome = iota
	resolveOutcomeFailed
	resolveOutcomeCancelled
	resolveOutcomeLate
)

// resolve marks a pending continuation as resolved. It closes the continuation's done
// channel (exactly once) and returns the outcome kind and whether the entry was found.
func (r *continuationRegistry) resolve(id ContinuationID, kind ContinuationOutcomeKind) (resolveOutcome, bool) {
	r.mu.Lock()
	entry, ok := r.pending[id]
	if !ok {
		r.mu.Unlock()
		r.lateCompletions.Add(1)
		return resolveOutcomeLate, false
	}
	delete(r.pending, id)
	r.perShard[entry.shardID]--
	if r.perShard[entry.shardID] == 0 {
		delete(r.perShard, entry.shardID)
	}
	r.mu.Unlock()

	entry.closeDone()

	switch kind {
	case continuationOutcomeComplete:
		r.completed.Add(1)
		return resolveOutcomeCompleted, true
	case continuationOutcomeFail:
		r.failed.Add(1)
		return resolveOutcomeFailed, true
	default:
		r.cancelled.Add(1)
		return resolveOutcomeCancelled, true
	}
}

// recordResumeRejected increments the resume-rejected counter.
func (r *continuationRegistry) recordResumeRejected() {
	r.resumeRejected.Add(1)
}

// recordLate increments the late-completion counter (completer called after registry resolve).
func (r *continuationRegistry) recordLate() {
	r.lateCompletions.Add(1)
}

// snapshot returns a copy-out diagnostic snapshot.
func (r *continuationRegistry) snapshot() ContinuationSnapshot {
	r.mu.Lock()
	pending := len(r.pending)
	r.mu.Unlock()
	return ContinuationSnapshot{
		Pending:         pending,
		MaxPending:      r.cfg.MaxPending,
		Completed:       r.completed.Load(),
		Failed:          r.failed.Load(),
		Cancelled:       r.cancelled.Load(),
		LateCompletions: r.lateCompletions.Load(),
		ResumeRejected:  r.resumeRejected.Load(),
	}
}

// shardSnapshot returns per-shard pending diagnostics (copy-out).
func (r *continuationRegistry) shardSnapshot() []ShardContinuationSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.perShard) == 0 {
		return nil
	}
	out := make([]ShardContinuationSnapshot, 0, len(r.perShard))
	for shardID, count := range r.perShard {
		out = append(out, ShardContinuationSnapshot{
			ShardID:    shardID,
			Pending:    count,
			MaxPending: r.cfg.MaxPendingPerShard,
		})
	}
	return out
}
