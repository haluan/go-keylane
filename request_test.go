// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"errors"
	"testing"
)

type sumInput struct {
	A int
	B int
}

type sumOutput struct {
	Sum int
}

func TestSubmitRequestNilQueue(t *testing.T) {
	f, err := SubmitRequest(context.Background(), nil, Request[sumInput, sumOutput]{})
	if f == nil {
		t.Fatal("future should not be nil")
	}
	if !errors.Is(err, ErrNilQueue) {
		t.Errorf("got %v, want %v", err, ErrNilQueue)
	}
	_, awaitErr := f.Await(context.Background())
	if !errors.Is(awaitErr, ErrNilQueue) {
		t.Errorf("Await got %v", awaitErr)
	}
}

func TestSubmitRequestValidation(t *testing.T) {
	q, _ := New(Config{
		ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1,
		LaneQuotas: map[Lane]int{"test": 1},
	})
	validHandle := func(context.Context, sumInput) (sumOutput, error) {
		return sumOutput{}, nil
	}

	tests := []struct {
		name string
		req  Request[sumInput, sumOutput]
		want error
	}{
		{
			name: "empty key",
			req: Request[sumInput, sumOutput]{
				Meta:   RequestMeta{Key: "", Lane: "test"},
				Handle: validHandle,
			},
			want: ErrInvalidKey,
		},
		{
			name: "empty lane",
			req: Request[sumInput, sumOutput]{
				Meta:   RequestMeta{Key: "k", Lane: ""},
				Handle: validHandle,
			},
			want: ErrInvalidLane,
		},
		{
			name: "nil handler",
			req: Request[sumInput, sumOutput]{
				Meta: RequestMeta{Key: "k", Lane: "test"},
			},
			want: ErrNilJobRun,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := SubmitRequest(context.Background(), q, tt.req)
			if !errors.Is(err, tt.want) {
				t.Errorf("got %v, want %v", err, tt.want)
			}
		})
	}
}

func TestSubmitRequestTypedOutput(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	future, err := SubmitRequest(ctx, q, Request[sumInput, sumOutput]{
		Meta: RequestMeta{
			RequestID: "req-123",
			Key:       "tenant-1",
			Lane:      "default",
		},
		Input: sumInput{A: 2, B: 3},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			return sumOutput{Sum: in.A + in.B}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}

	out, err := future.Await(ctx)
	if err != nil {
		t.Fatalf("Await: %v", err)
	}
	if out.Sum != 5 {
		t.Errorf("Sum = %d, want 5", out.Sum)
	}
}

func TestSubmitRequestHandlerError(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	errExpected := errors.New("handler failed")
	future, _ := SubmitRequest(ctx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			return sumOutput{}, errExpected
		},
	})

	_, err := future.Await(ctx)
	if !errors.Is(err, errExpected) {
		t.Errorf("got %v, want %v", err, errExpected)
	}
}

func TestSubmitRequestShardRoutingSameKey(t *testing.T) {
	ctx := testTimeout(t)
	cfg := newTestConfig()
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	key := "test-key"
	handle := func(context.Context, sumInput) (sumOutput, error) {
		return sumOutput{}, nil
	}

	for i := 0; i < 2; i++ {
		_, err := SubmitRequest(ctx, q, Request[sumInput, sumOutput]{
			Meta:   RequestMeta{Key: key, Lane: "default"},
			Input:  sumInput{},
			Handle: handle,
		})
		if err != nil {
			t.Fatalf("SubmitRequest %d: %v", i, err)
		}
	}

	stats := q.Stats()
	var activeShardID = -1
	for _, ss := range stats.Shards {
		if ss.TotalDepth > 0 {
			if activeShardID == -1 {
				activeShardID = ss.ShardID
			} else if activeShardID != ss.ShardID {
				t.Fatalf("same key routed to different shards: %d and %d", activeShardID, ss.ShardID)
			}
		}
	}
	if activeShardID == -1 {
		t.Fatal("expected at least one shard with queued work")
	}
}

func TestSubmitRequestLaneRouting(t *testing.T) {
	ctx := testTimeout(t)
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas: map[Lane]int{
			"default": 1,
			"payment": 1,
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = SubmitRequest(ctx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "payment"},
		Input: sumInput{},
		Handle: func(context.Context, sumInput) (sumOutput, error) {
			return sumOutput{Sum: 1}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}

	stats := q.Stats()
	paymentDepth := laneDepthInStats(stats, "payment")
	defaultDepth := laneDepthInStats(stats, "default")
	if paymentDepth < 1 {
		t.Errorf("payment lane depth = %d, want >= 1", paymentDepth)
	}
	if defaultDepth != 0 {
		t.Errorf("default lane depth = %d, want 0", defaultDepth)
	}
}

func laneDepthInStats(stats Stats, lane Lane) int {
	for _, ss := range stats.Shards {
		for _, ls := range ss.Lanes {
			if ls.Lane == lane {
				return ls.Depth
			}
		}
	}
	return 0
}

func laneStatsInStats(stats Stats, lane Lane) (LaneStats, bool) {
	for _, ss := range stats.Shards {
		for _, ls := range ss.Lanes {
			if ls.Lane == lane {
				return ls, true
			}
		}
	}
	return LaneStats{}, false
}

func TestSubmitRequestTimingPreserved(t *testing.T) {
	ctx := testTimeout(t)
	cfg := Config{
		ShardCount:       1,
		WorkerCount:      1,
		QueueSizePerLane: 10,
		LaneQuotas:       map[Lane]int{"default": 1},
		Observability: ObservabilityConfig{
			TrackQueueWait: true,
		},
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := q.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	future, err := SubmitRequest(ctx, q, Request[sumInput, sumOutput]{
		Meta:  RequestMeta{Key: "k", Lane: "default"},
		Input: sumInput{A: 1, B: 1},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			return sumOutput{Sum: in.A + in.B}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}
	if _, err := future.Await(ctx); err != nil {
		t.Fatalf("Await: %v", err)
	}

	stats := q.Stats()
	ls, ok := laneStatsInStats(stats, "default")
	if !ok {
		t.Fatal("default lane not found in stats")
	}
	if ls.CompletedTotal < 1 {
		t.Errorf("CompletedTotal = %d, want >= 1", ls.CompletedTotal)
	}
	if ls.QueueWaitCount < 1 {
		t.Errorf("QueueWaitCount = %d, want >= 1", ls.QueueWaitCount)
	}
}

func TestSubmitRequestWithRequestID(t *testing.T) {
	ctx := testTimeout(t)
	q := newStartedTestQueue(t, ctx)

	future, err := SubmitRequest(ctx, q, Request[sumInput, sumOutput]{
		Meta: RequestMeta{
			RequestID: "req-1",
			Key:       "k",
			Lane:      "default",
		},
		Input: sumInput{A: 1, B: 2},
		Handle: func(ctx context.Context, in sumInput) (sumOutput, error) {
			return sumOutput{Sum: in.A + in.B}, nil
		},
	})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}
	if _, err := future.Await(ctx); err != nil {
		t.Fatalf("Await: %v", err)
	}
}
