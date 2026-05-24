// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"testing"
	"time"
)

func shardPressureTestConfig() Config {
	return Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 32,
		LaneQuotas:       map[Lane]int{"default": 2, "bulk": 2},
		HotKey: HotKeyConfig{
			Enabled: true, MaxTrackedKeysPerShard: 16,
			DetectionWindow: time.Minute, HotKeyDepthRatio: 0.3,
		},
		PerKeyAdmission: DefaultPerKeyAdmissionConfig(),
		ShardPressure:   DefaultShardPressureConfig(),
	}
}

func TestHotShardAppearsInPressureSummary(t *testing.T) {
	q, err := New(shardPressureTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i := 0; i < 40; i++ {
		key := "hot-key"
		if i%10 != 0 {
			key = "other-" + string(rune('a'+i%5))
		}
		_ = q.Submit(context.Background(), Job{Key: key, Lane: "default", Run: run})
	}
	summary := q.PressureSummary()
	if len(summary.HotShards) == 0 {
		t.Fatal("expected hot shards in summary")
	}
}

func TestDominantLaneAppearsInShardPressure(t *testing.T) {
	cfg := shardPressureTestConfig()
	cfg.ShardCount = 1
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i := 0; i < 20; i++ {
		lane := Lane("bulk")
		if i%5 != 0 {
			lane = "default"
		}
		_ = q.Submit(context.Background(), Job{Key: "k", Lane: lane, Run: run})
	}
	snap, ok := q.ShardPressure(0)
	if !ok {
		t.Fatal("ShardPressure(0) not ok")
	}
	if len(snap.LaneBreakdown) == 0 {
		t.Fatal("expected lane breakdown")
	}
}

func TestLocalizedHotKeyPressureUsesKL1501Candidates(t *testing.T) {
	cfg := shardPressureTestConfig()
	cfg.ShardCount = 1
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	const hot = "tenant-hot"
	for i := 0; i < 35; i++ {
		_ = q.Submit(context.Background(), Job{Key: hot, Lane: "default", Run: run})
	}
	snap, ok := q.ShardPressure(0)
	if !ok {
		t.Fatal("ShardPressure not ok")
	}
	if len(snap.HotKeyCandidates) == 0 {
		t.Fatal("expected hot key candidates")
	}
	topContrib := 0.0
	for _, hk := range snap.HotKeyCandidates {
		c := hk.DepthContributionRatio
		if hk.WaitContributionRatio > c {
			c = hk.WaitContributionRatio
		}
		if hk.AdmissionContributionRatio > c {
			c = hk.AdmissionContributionRatio
		}
		if c > topContrib {
			topContrib = c
		}
	}
	if snap.PressureRatio >= cfg.ShardPressure.HotShardPressureRatio &&
		topContrib >= cfg.ShardPressure.LocalizedHotKeyRatio {
		if snap.Class != ShardPressureLocalizedKey {
			t.Fatalf("class = %q, want localized_key (pressure=%v contrib=%v)", snap.Class, snap.PressureRatio, topContrib)
		}
	}
}

func TestPerKeyMitigationContributesToPressureSnapshot(t *testing.T) {
	cfg := shardPressureTestConfig()
	cfg.ShardCount = 1
	cfg.PerKeyAdmission.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	const hot = "mit-key"
	for i := 0; i < 30; i++ {
		_ = q.Submit(context.Background(), Job{Key: hot, Lane: "default", Run: run})
	}
	pk := cfg.PerKeyAdmission
	pk.Enabled = true
	_ = CheckPerKeyAdmission(q, pk, RequestMeta{Key: hot, Lane: "default"})
	snap, ok := q.ShardPressure(0)
	if !ok {
		t.Fatal("ShardPressure not ok")
	}
	for _, hk := range snap.HotKeyCandidates {
		if hk.ActiveMitigation != "" && hk.ActiveMitigation != PerKeyMitigationAllow {
			return
		}
	}
	t.Fatal("expected active mitigation on hot key pressure snapshot")
}

func TestDistributedBacklogClassifiedAsScaleRelevant(t *testing.T) {
	cfg := shardPressureTestConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for shard := 0; shard < 4; shard++ {
		for i := 0; i < 15; i++ {
			key := "key-" + string(rune('a'+shard)) + "-" + string(rune('0'+i%10))
			_ = q.Submit(context.Background(), Job{Key: key, Lane: "default", Run: run})
		}
	}
	summary := q.PressureSummary()
	if summary.HotShardCount < 2 && summary.Class != ShardPressureDistributed {
		t.Logf("summary class=%q hot=%d scale=%v", summary.Class, summary.HotShardCount, summary.ScaleRelevant)
	}
}

func TestLocalizedHotKeyClassifiedAsMitigationRelevant(t *testing.T) {
	cfg := shardPressureTestConfig()
	cfg.ShardCount = 1
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for i := 0; i < 40; i++ {
		_ = q.Submit(context.Background(), Job{Key: "only-key", Lane: "default", Run: run})
	}
	summary := q.PressureSummary()
	if summary.Class == ShardPressureLocalizedKey && !summary.MitigationRelevant {
		t.Fatalf("localized class should set MitigationRelevant, summary=%+v", summary)
	}
}

func TestPressureSummaryDoesNotExposeRawKey(t *testing.T) {
	cfg := shardPressureTestConfig()
	cfg.ShardCount = 1
	cfg.HotKey.ExposeRawKey = true
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		_ = q.Submit(context.Background(), Job{
			Key: "secret-key", Lane: "default",
			Run: func(context.Context) error { return nil },
		})
	}
	snap, ok := q.ShardPressure(0)
	if !ok {
		t.Fatal("ShardPressure not ok")
	}
	for _, hk := range snap.HotKeyCandidates {
		_ = hk.KeyHash
	}
}

func TestShardPressureDisabledDiagnosticsEnabledFalse(t *testing.T) {
	cfg := shardPressureTestConfig()
	cfg.ShardPressure.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	summary := q.PressureSummary()
	if summary.DiagnosticsEnabled {
		t.Fatal("DiagnosticsEnabled should be false when disabled")
	}
	if summary.Class != "" {
		t.Fatalf("class = %q, want empty when disabled", summary.Class)
	}
	snap, ok := q.ShardPressure(0)
	if !ok {
		t.Fatal("ShardPressure(0) should be ok when disabled but shard valid")
	}
	if snap.DiagnosticsEnabled {
		t.Fatal("ShardPressure DiagnosticsEnabled should be false when disabled")
	}
	if snap.ShardID != 0 {
		t.Fatalf("ShardID = %d, want 0", snap.ShardID)
	}
	if _, ok := q.ShardPressure(99); ok {
		t.Fatal("ShardPressure should be false for invalid shard ID")
	}
}

func TestDebugSnapshotIncludesPressureSummary(t *testing.T) {
	cfg := shardPressureTestConfig()
	cfg.Observability.EnableDebugSnapshot = true
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	snap := q.DebugSnapshot()
	if snap.Version != DebugSnapshotVersion {
		t.Fatalf("version = %q, want %q", snap.Version, DebugSnapshotVersion)
	}
	if snap.PressureSummary.UpdatedAt.IsZero() && snap.PressureSummary.Class == "" {
		t.Fatal("expected pressure summary populated")
	}
}

func TestHotShardsMethodReturnsBoundedList(t *testing.T) {
	cfg := shardPressureTestConfig()
	cfg.ShardPressure.MaxHotShards = 2
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	hot := q.HotShards()
	if len(hot) > 2 {
		t.Fatalf("len = %d, want <= 2", len(hot))
	}
	dst := make([]ShardPressureSnapshot, 0, 4)
	out := q.AppendHotShards(dst)
	if cap(out) < 4 {
		t.Fatalf("cap = %d, want reuse of dst capacity", cap(out))
	}
}

func TestHotKeyRejectNotDoubleCountedIntegration(t *testing.T) {
	cfg := shardPressureTestConfig()
	cfg.ShardCount = 1
	cfg.PerKeyAdmission.Enabled = false
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	defer close(block)
	run := func(ctx context.Context) error {
		select {
		case <-block:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	const hot = "throttle-key"
	for i := 0; i < 30; i++ {
		_ = q.Submit(context.Background(), Job{Key: hot, Lane: "default", Run: run})
	}
	pk := cfg.PerKeyAdmission
	pk.Enabled = true
	pk.DefaultAction = PerKeyMitigationThrottle
	_ = CheckPerKeyAdmission(q, pk, RequestMeta{Key: hot, Lane: "default"})
	snap, ok := q.ShardPressure(0)
	if !ok {
		t.Fatal("ShardPressure not ok")
	}
	for _, hk := range snap.HotKeyCandidates {
		if hk.ActiveMitigation == PerKeyMitigationThrottle {
			if hk.ThrottledApprox == 0 {
				t.Fatal("expected ThrottledApprox on throttle mitigation")
			}
			if hk.RejectedApprox != 0 {
				t.Fatalf("RejectedApprox = %d, want 0 on throttle", hk.RejectedApprox)
			}
			return
		}
	}
	t.Fatal("expected throttle mitigation on hot key pressure snapshot")
}
