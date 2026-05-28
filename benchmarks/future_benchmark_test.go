// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks_test

import (
	"context"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

func BenchmarkKeylaneSubmitValue(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfigSingleLane())
	job := keylane.ValueJob[int]{Key: "k", Lane: "default", Run: dummyValueRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = keylane.SubmitValue(context.Background(), q, job)
	}
}

func BenchmarkKeylaneSubmitValueManyKeys(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfig())
	keys := generateManyKeys(256, 2)
	job := keylane.ValueJob[int]{Lane: "default", Run: dummyValueRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job.Key = keys[i%len(keys)]
		_, _ = keylane.SubmitValue(context.Background(), q, job)
	}
}

func BenchmarkKeylaneSubmitValueOneHotKey(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfig())
	job := keylane.ValueJob[int]{Key: generateHotKey(), Lane: "default", Run: dummyValueRun}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = keylane.SubmitValue(context.Background(), q, job)
	}
}

func BenchmarkKeylaneAwaitCompleted(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfigSingleLane())
	job := keylane.ValueJob[int]{Key: "k", Lane: "default", Run: dummyValueRun}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := keylane.SubmitValue(ctx, q, job)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = f.Await(ctx)
	}
}

func BenchmarkKeylaneAwaitCompletedWithError(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfigSingleLane())
	job := keylane.ValueJob[int]{
		Key:  "k",
		Lane: "default",
		Run: func(ctx context.Context) (int, error) {
			return 0, context.Canceled
		},
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := keylane.SubmitValue(ctx, q, job)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = f.Await(ctx)
	}
}

func BenchmarkKeylaneAwaitBlocking(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfigSingleLane())
	ctx := context.Background()
	job := keylane.ValueJob[int]{
		Key:  "block",
		Lane: "default",
		Run: func(ctx context.Context) (int, error) {
			time.Sleep(100 * time.Microsecond)
			return 1, nil
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := keylane.SubmitValue(ctx, q, job)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = f.Await(ctx)
	}
}

const benchAwaitFollowers = 16

func BenchmarkKeylaneAwaitManyFollowersCompleted(b *testing.B) {
	q, _ := makeBenchmarkQueue(b, benchConfigSingleLane())
	job := keylane.ValueJob[int]{Key: "k", Lane: "default", Run: dummyValueRun}
	ctx := context.Background()

	f, err := keylane.SubmitValue(ctx, q, job)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := f.Await(ctx); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < benchAwaitFollowers; j++ {
			_, _ = f.Await(ctx)
		}
	}
}

func BenchmarkKeylaneAwaitTimeout(b *testing.B) {
	q, cancel := makeBenchmarkQueue(b, benchConfigSingleLane())
	ctx := context.Background()
	block := make(chan struct{})
	b.Cleanup(func() {
		close(block)
		cancel()
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job := keylane.ValueJob[int]{
			Key:  "slow",
			Lane: "default",
			Run: func(ctx context.Context) (int, error) {
				<-block
				return 1, nil
			},
		}
		f, err := keylane.SubmitValue(ctx, q, job)
		if err != nil {
			b.Fatal(err)
		}
		awaitCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
		_, _ = f.Await(awaitCtx)
		cancel()
	}
}
