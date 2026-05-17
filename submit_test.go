package keylane

import (
	"context"
	"errors"
	"testing"
)

func TestSubmitAcceptsValidJob(t *testing.T) {
	q, _ := New(Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}})

	err := q.Submit(context.Background(), Job{
		Key:  "k",
		Lane: "test",
		Run:  func(ctx context.Context) error { return nil },
	})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestSubmitContextCancelled(t *testing.T) {
	q, _ := New(Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := q.Submit(ctx, Job{
		Key:  "k",
		Lane: "test",
		Run:  func(ctx context.Context) error { return nil },
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got error %v, want %v", err, context.Canceled)
	}
}

func TestSubmitValidation(t *testing.T) {
	q, _ := New(Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}})

	tests := []struct {
		name string
		job  Job
		want error
	}{
		{
			name: "empty key",
			job:  Job{Key: "", Lane: "test", Run: func(ctx context.Context) error { return nil }},
			want: ErrInvalidKey,
		},
		{
			name: "empty lane",
			job:  Job{Key: "k", Lane: "", Run: func(ctx context.Context) error { return nil }},
			want: ErrInvalidLane,
		},
		{
			name: "nil run",
			job:  Job{Key: "k", Lane: "test", Run: nil},
			want: ErrNilJobRun,
		},
		{
			name: "unknown lane",
			job:  Job{Key: "k", Lane: "unknown", Run: func(ctx context.Context) error { return nil }},
			want: ErrInvalidLane,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := q.Submit(context.Background(), tt.job)
			if !errors.Is(err, tt.want) {
				t.Errorf("got error %v, want %v", err, tt.want)
			}
		})
	}
}

func TestSubmitQueueFull(t *testing.T) {
	q, _ := New(Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 1, LaneQuotas: map[Lane]int{"test": 1}})

	job := Job{
		Key:  "k",
		Lane: "test",
		Run:  func(ctx context.Context) error { return nil },
	}
	_ = q.Submit(context.Background(), job)
	err := q.Submit(context.Background(), job)
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("got error %v, want %v", err, ErrQueueFull)
	}
}

func TestSubmitNotifiesReadyOncePerShard(t *testing.T) {
	q, _ := New(Config{ShardCount: 1, WorkerCount: 1, QueueSizePerLane: 10, LaneQuotas: map[Lane]int{"test": 1}})

	job := Job{Key: "k", Lane: "test", Run: func(ctx context.Context) error { return nil }}

	_ = q.Submit(context.Background(), job)
	if len(q.sched.ReadyCh) != 1 {
		t.Errorf("ReadyCh len = %d, want 1", len(q.sched.ReadyCh))
	}

	_ = q.Submit(context.Background(), job)
	if len(q.sched.ReadyCh) != 1 {
		t.Errorf("ReadyCh len after second submit = %d, want 1", len(q.sched.ReadyCh))
	}
}
