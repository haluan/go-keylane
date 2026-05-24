// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"sync"
	"time"
)

type hotKeyEntry struct {
	keyHash uint64
	laneID  LaneID

	submittedApprox uint64
	queuedApprox    int64
	rejectedApprox  uint64

	queueWaitApproxNanos uint64
	lastSeenUnixNano     int64
	epoch                uint64

	inflightApprox int64

	lastAction            PerKeyMitigationAction
	lastReason            PerKeyAdmissionReason
	lastDecisionUnixNano  int64
	cooldownUntilUnixNano int64
	recoveryUntilUnixNano int64
}

type hotKeyTracker struct {
	mu      sync.Mutex
	cfg     HotKeyConfig
	entries []hotKeyEntry
	index   map[uint64]int
	rawKeys []string // parallel to entries when ExposeRawKey; empty slot = ""
	epoch   uint64
}

func newHotKeyTracker(cfg HotKeyConfig) *hotKeyTracker {
	if !cfg.Enabled || cfg.MaxTrackedKeysPerShard <= 0 {
		return &hotKeyTracker{cfg: HotKeyConfig{Enabled: false}}
	}
	n := cfg.MaxTrackedKeysPerShard
	t := &hotKeyTracker{
		cfg:     cfg,
		entries: make([]hotKeyEntry, n),
		index:   make(map[uint64]int, n),
	}
	if cfg.ExposeRawKey {
		t.rawKeys = make([]string, n)
	}
	return t
}

func (t *hotKeyTracker) enabled() bool {
	return t != nil && t.cfg.Enabled && t.cfg.MaxTrackedKeysPerShard > 0 && t.index != nil
}

func (t *hotKeyTracker) observeSubmit(keyHash uint64, laneID LaneID, rawKey string, now time.Time) {
	if t == nil || !t.enabled() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireStaleEntriesLocked(now)
	e := t.touch(keyHash, laneID, rawKey, now)
	e.submittedApprox++
}

func (t *hotKeyTracker) observeEnqueue(keyHash uint64, laneID LaneID, rawKey string, now time.Time) {
	if t == nil || !t.enabled() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.expireStaleEntriesLocked(now)
	e := t.touch(keyHash, laneID, rawKey, now)
	e.queuedApprox++
}

func (t *hotKeyTracker) observeDequeue(keyHash uint64, now time.Time) {
	if t == nil || !t.enabled() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entryIfFresh(keyHash, now)
	if e == nil {
		return
	}
	if e.queuedApprox > 0 {
		e.queuedApprox--
	}
	e.lastSeenUnixNano = now.UnixNano()
}

func (t *hotKeyTracker) observeReject(keyHash uint64, now time.Time) {
	if t == nil || !t.enabled() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entryIfFresh(keyHash, now)
	if e == nil {
		return
	}
	e.rejectedApprox++
	e.lastSeenUnixNano = now.UnixNano()
}

func (t *hotKeyTracker) observeQueueWait(keyHash uint64, waitNanos uint64, now time.Time) {
	if t == nil || !t.enabled() || waitNanos == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entryIfFresh(keyHash, now)
	if e == nil {
		return
	}
	e.queueWaitApproxNanos += waitNanos
	e.lastSeenUnixNano = now.UnixNano()
}

func (t *hotKeyTracker) touch(keyHash uint64, laneID LaneID, rawKey string, now time.Time) *hotKeyEntry {
	if idx, ok := t.index[keyHash]; ok {
		e := &t.entries[idx]
		if e.keyHash != keyHash {
			delete(t.index, keyHash)
		} else {
			t.maybeResetEntry(e, now)
			if e.keyHash == 0 {
				t.initSlot(idx, keyHash, laneID, rawKey, now)
				return &t.entries[idx]
			}
			e.laneID = laneID
			e.lastSeenUnixNano = now.UnixNano()
			if t.cfg.ExposeRawKey && rawKey != "" {
				t.rawKeys[idx] = rawKey
			}
			return e
		}
	}
	idx := t.allocateSlot(keyHash, laneID, rawKey, now)
	return &t.entries[idx]
}

func (t *hotKeyTracker) entryIfFresh(keyHash uint64, now time.Time) *hotKeyEntry {
	idx, ok := t.index[keyHash]
	if !ok {
		return nil
	}
	e := &t.entries[idx]
	if e.keyHash != keyHash {
		return nil
	}
	t.maybeResetEntry(e, now)
	return e
}

func (t *hotKeyTracker) maybeResetEntry(e *hotKeyEntry, now time.Time) {
	if e.keyHash == 0 {
		return
	}
	window := t.cfg.DetectionWindow
	if window <= 0 {
		return
	}
	if e.lastSeenUnixNano == 0 {
		return
	}
	if now.Sub(time.Unix(0, e.lastSeenUnixNano)) > window {
		t.clearEntry(e)
		e.epoch = t.epoch
	}
}

func (t *hotKeyTracker) clearEntry(e *hotKeyEntry) {
	if e.keyHash != 0 {
		delete(t.index, e.keyHash)
		if t.rawKeys != nil {
			for i := range t.entries {
				if &t.entries[i] == e {
					t.rawKeys[i] = ""
					break
				}
			}
		}
	}
	*e = hotKeyEntry{epoch: t.epoch}
}

func (t *hotKeyTracker) activeCount() int {
	return len(t.index)
}

func (t *hotKeyTracker) allocateSlot(keyHash uint64, laneID LaneID, rawKey string, now time.Time) int {
	n := len(t.entries)
	for i := range t.entries {
		if t.entries[i].keyHash == 0 {
			t.removeSlotFromIndex(i)
			t.initSlot(i, keyHash, laneID, rawKey, now)
			return i
		}
	}
	// At capacity: evict LRU by lastSeenUnixNano (deterministic scan).
	evict := 0
	evictSeen := t.entries[0].lastSeenUnixNano
	for i := 1; i < n; i++ {
		seen := t.entries[i].lastSeenUnixNano
		if seen < evictSeen {
			evict = i
			evictSeen = seen
		}
	}
	t.removeSlotFromIndex(evict)
	t.initSlot(evict, keyHash, laneID, rawKey, now)
	return evict
}

func (t *hotKeyTracker) removeSlotFromIndex(i int) {
	old := t.entries[i]
	if old.keyHash != 0 {
		delete(t.index, old.keyHash)
		return
	}
	// Cleared slot may still be referenced by stale index entries after window reset.
	for h, idx := range t.index {
		if idx == i {
			delete(t.index, h)
		}
	}
}

func (t *hotKeyTracker) expireStaleEntriesLocked(now time.Time) {
	window := t.cfg.DetectionWindow
	if window <= 0 {
		return
	}
	for i := range t.entries {
		e := &t.entries[i]
		if e.keyHash == 0 {
			continue
		}
		if e.lastSeenUnixNano == 0 {
			continue
		}
		if now.Sub(time.Unix(0, e.lastSeenUnixNano)) > window {
			t.clearEntry(e)
		}
	}
}

func (t *hotKeyTracker) initSlot(i int, keyHash uint64, laneID LaneID, rawKey string, now time.Time) {
	t.entries[i] = hotKeyEntry{
		keyHash:          keyHash,
		laneID:           laneID,
		lastSeenUnixNano: now.UnixNano(),
		epoch:            t.epoch,
	}
	t.index[keyHash] = i
	if t.rawKeys != nil && rawKey != "" {
		t.rawKeys[i] = rawKey
	}
}

func (t *hotKeyTracker) len() int {
	if t == nil || !t.enabled() {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.activeCount()
}
