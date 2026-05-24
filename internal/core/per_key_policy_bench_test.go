// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"testing"
	"time"
)

func benchPerKeyEvaluate(b *testing.B, setup func(tr *hotKeyTracker, now time.Time), keyHash uint64) {
	tr := newHotKeyTracker(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 64,
		DetectionWindow: 30 * time.Second, HotKeyDepthRatio: 0.4,
	})
	pkCfg := PerKeyAdmissionConfig{
		Enabled: true, MinStatus: HotKeyStatusCandidate,
		DefaultAction: PerKeyMitigationThrottle, PressureRatioThreshold: 0.4,
	}
	now := time.Now()
	setup(tr, now)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tr.evaluatePerKeyAdmission(0, keyHash, 0, 64, 0, 0.5, pkCfg, now)
	}
}

func BenchmarkPerKeyEvaluateAdmission(b *testing.B) {
	benchPerKeyEvaluate(b, func(tr *hotKeyTracker, now time.Time) {
		for i := 0; i < 64; i++ {
			tr.observeSubmit(uint64(i), 0, "", now)
			tr.observeEnqueue(uint64(i), 0, "", now)
		}
	}, 0)
}

func BenchmarkPerKeyEvaluateColdKey(b *testing.B) {
	benchPerKeyEvaluate(b, func(tr *hotKeyTracker, now time.Time) {
		tr.observeSubmit(1, 0, "", now)
		tr.observeEnqueue(1, 0, "", now)
	}, 99)
}

func BenchmarkPerKeyEvaluateDominantHotKey(b *testing.B) {
	const hot uint64 = 42
	benchPerKeyEvaluate(b, func(tr *hotKeyTracker, now time.Time) {
		for i := 0; i < 50; i++ {
			tr.observeSubmit(hot, 0, "", now)
			tr.observeEnqueue(hot, 0, "", now)
		}
		for i := uint64(1); i < 5; i++ {
			tr.observeSubmit(i, 0, "", now)
			tr.observeEnqueue(i, 0, "", now)
		}
	}, hot)
}

func BenchmarkPerKeyEvaluateManyKeysBeyondCap(b *testing.B) {
	tr := newHotKeyTracker(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 8,
		DetectionWindow: 30 * time.Second, HotKeyDepthRatio: 0.4,
	})
	pkCfg := PerKeyAdmissionConfig{
		Enabled: true, MinStatus: HotKeyStatusCandidate,
		DefaultAction: PerKeyMitigationThrottle, PressureRatioThreshold: 0.4,
	}
	now := time.Now()
	for i := 0; i < 32; i++ {
		h := uint64(i + 1000)
		tr.observeSubmit(h, 0, "", now)
		tr.observeEnqueue(h, 0, "", now)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := uint64(i%32 + 1000)
		_ = tr.evaluatePerKeyAdmission(0, h, 0, 32, 0, 0.5, pkCfg, now)
	}
}

func BenchmarkPerKeyEvaluateThrottle(b *testing.B) {
	const hot uint64 = 7
	tr := newHotKeyTracker(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 16,
		DetectionWindow: time.Minute, HotKeyDepthRatio: 0.3,
	})
	pkCfg := PerKeyAdmissionConfig{
		Enabled: true, DefaultAction: PerKeyMitigationThrottle,
		MinStatus: HotKeyStatusCandidate, PressureRatioThreshold: 0.35,
	}
	now := time.Now()
	for i := 0; i < 30; i++ {
		tr.observeSubmit(hot, 0, "", now)
		tr.observeEnqueue(hot, 0, "", now)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tr.evaluatePerKeyAdmission(0, hot, 0, 30, 0, 0, pkCfg, now)
	}
}

func BenchmarkPerKeyEvaluateReject(b *testing.B) {
	const hot uint64 = 7
	tr := newHotKeyTracker(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 16,
		DetectionWindow: time.Minute, HotKeyDepthRatio: 0.3,
	})
	pkCfg := PerKeyAdmissionConfig{
		Enabled: true, DefaultAction: PerKeyMitigationReject,
		MinStatus: HotKeyStatusCandidate, PressureRatioThreshold: 0.35,
	}
	now := time.Now()
	for i := 0; i < 30; i++ {
		tr.observeSubmit(hot, 0, "", now)
		tr.observeEnqueue(hot, 0, "", now)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tr.evaluatePerKeyAdmission(0, hot, 0, 30, 0, 0, pkCfg, now)
	}
}

func BenchmarkPerKeyEvaluateManyUniqueBeyondCap(b *testing.B) {
	tr := newHotKeyTracker(HotKeyConfig{
		Enabled: true, MaxTrackedKeysPerShard: 8,
		DetectionWindow: 30 * time.Second, HotKeyDepthRatio: 0.4,
	})
	pkCfg := PerKeyAdmissionConfig{Enabled: true, PressureRatioThreshold: 0.4}
	now := time.Now()
	for i := 0; i < 64; i++ {
		h := uint64(i + 5000)
		tr.observeSubmit(h, 0, fmt.Sprintf("k%d", i), now)
		tr.observeEnqueue(h, 0, "", now)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := uint64(i%64 + 5000)
		_ = tr.evaluatePerKeyAdmission(0, h, 0, 64, 0, 0.5, pkCfg, now)
	}
}
