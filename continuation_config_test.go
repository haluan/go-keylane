// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNormalizeContinuationConfigDefaultMaxPending(t *testing.T) {
	cfg := ContinuationConfig{Enabled: true}
	NormalizeContinuationConfig(&cfg)
	if cfg.MaxPending != DefaultContinuationMaxPending {
		t.Fatalf("MaxPending = %d, want %d", cfg.MaxPending, DefaultContinuationMaxPending)
	}
	if err := ValidateContinuationConfig(cfg); err != nil {
		t.Fatal(err)
	}
}

func TestNewQueueNormalizesContinuationMaxPending(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	cfg.Continuation = ContinuationConfig{Enabled: true}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Stop(context.Background()) })

	snap := q.DebugSnapshot().Continuation
	if snap.MaxPending != DefaultContinuationMaxPending {
		t.Fatalf("snapshot MaxPending = %d, want %d", snap.MaxPending, DefaultContinuationMaxPending)
	}
}

func TestContinuationRegistryRejectsBeyondNormalizedMaxPending(t *testing.T) {
	reg := newContinuationRegistry(ContinuationConfig{Enabled: true, MaxPending: DefaultContinuationMaxPending})
	makeEntry := func(id ContinuationID) pendingEntry {
		done := make(chan struct{})
		var once sync.Once
		return pendingEntry{
			id:           id,
			shardID:      0,
			registeredAt: time.Now(),
			closeDone:    func() { once.Do(func() { close(done) }) },
		}
	}
	for i := 1; i <= DefaultContinuationMaxPending; i++ {
		if err := reg.register(makeEntry(ContinuationID(i))); err != nil {
			t.Fatalf("register %d: %v", i, err)
		}
	}
	if err := reg.register(makeEntry(ContinuationID(DefaultContinuationMaxPending + 1))); err != ErrContinuationLimitExceeded {
		t.Fatalf("expected ErrContinuationLimitExceeded, got %v", err)
	}
}
