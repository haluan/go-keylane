// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"

	"github.com/haluan/go-keylane/internal/core"
)

func TestDebugSnapshotV05TopLevelFieldsStableOrder(t *testing.T) {
	cfg := v05EnabledConfig()
	cfg.ShardCount = 2
	cfg.PerKeyAdmission.Enabled = false
	q, block := newV05ScenarioQueue(t, cfg)
	run := blockedRun(block)
	for i := 0; i < 30; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: "stable-hot", Lane: "default", Run: run,
		})
	}
	snap := q.DebugSnapshot()
	if len(snap.HotKeys) == 0 {
		t.Fatal("expected top-level HotKeys")
	}
	for i := 1; i < len(snap.HotKeys); i++ {
		a, b := snap.HotKeys[i-1], snap.HotKeys[i]
		if a.ShardID > b.ShardID || (a.ShardID == b.ShardID && a.KeyHash > b.KeyHash) {
			t.Fatalf("HotKeys not sorted at %d: %+v then %+v", i, a, b)
		}
	}
	if len(snap.ShardPressure) == 0 {
		t.Fatal("expected top-level ShardPressure slice")
	}
	for i := 1; i < len(snap.ShardPressure); i++ {
		if snap.ShardPressure[i-1].ShardID > snap.ShardPressure[i].ShardID {
			t.Fatal("ShardPressure not sorted by shard ID")
		}
	}
	if snap.HotKeys[0].Key != "" {
		t.Fatal("HotKeys must not expose raw key by default")
	}
	if snap.HotKeys[0].LastSeenUnixNano == 0 && snap.HotKeys[0].KeyHash == core.HashKey("stable-hot") {
		t.Fatal("expected LastSeenUnixNano for hot key candidate")
	}
}
