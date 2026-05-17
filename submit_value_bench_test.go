package keylane

import (
	"context"
	"testing"
)

func BenchmarkSubmitValue(b *testing.B) {
	q, cancel := setupQueue(16, 4, 10000, map[Lane]int{"default": 1})
	defer cancel()

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = SubmitValue(ctx, q, ValueJob[int]{
			Key:  "my-key",
			Lane: "default",
			Run:  dummyValueRun,
		})
	}
}

func BenchmarkSubmitValueWithoutPool(b *testing.B) {
	q, cancel := setupQueue(16, 4, 10000, map[Lane]int{"default": 1})
	defer cancel()

	// Access internal/private fields directly to disable pooling without exposing a public API leak
	q.sched.Obs.DisablePooling = true

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = SubmitValue(ctx, q, ValueJob[int]{
			Key:  "my-key",
			Lane: "default",
			Run:  dummyValueRun,
		})
	}
}

func BenchmarkSubmitValueWithPool(b *testing.B) {
	q, cancel := setupQueue(16, 4, 10000, map[Lane]int{"default": 1})
	defer cancel()

	// Access internal/private fields directly to enable pooling
	q.sched.Obs.DisablePooling = false

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = SubmitValue(ctx, q, ValueJob[int]{
			Key:  "my-key",
			Lane: "default",
			Run:  dummyValueRun,
		})
	}
}
