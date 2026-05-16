package keylane

import (
	"testing"
)

func TestNewQueue(t *testing.T) {
	cfg := Config{
		ShardCount:       64,
		WorkerCount:      4,
		QueueSizePerLane: 1024,
		LaneQuotas: map[Lane]int{
			"payment": 3,
			"audit":   1,
		},
	}

	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	if q == nil {
		t.Fatal("New() returned nil Queue")
	}

	if q.config.ShardCount != 64 {
		t.Errorf("ShardCount = %d, want 64", q.config.ShardCount)
	}
	if q.config.WorkerCount != 4 {
		t.Errorf("WorkerCount = %d, want 4", q.config.WorkerCount)
	}
	if q.config.QueueSizePerLane != 1024 {
		t.Errorf("QueueSizePerLane = %d, want 1024", q.config.QueueSizePerLane)
	}
}

func TestNewQueueInvalidConfig(t *testing.T) {
	cfg := Config{
		ShardCount: -1, // invalid
	}

	_, err := New(cfg)
	if err == nil {
		t.Error("New() with invalid config should return error")
	}
}
