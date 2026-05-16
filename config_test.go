package keylane

import (
	"errors"
	"testing"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr error
	}{
		{
			name: "valid config",
			config: Config{
				ShardCount:       1,
				WorkerCount:      1,
				QueueSizePerLane: 1,
				LaneQuotas:       map[Lane]int{"default": 1},
			},
			wantErr: nil,
		},
		{
			name: "zero shard count",
			config: Config{
				ShardCount:       0,
				WorkerCount:      1,
				QueueSizePerLane: 1,
				LaneQuotas:       map[Lane]int{"default": 1},
			},
			wantErr: ErrInvalidShardCount,
		},
		{
			name: "zero worker count",
			config: Config{
				ShardCount:       1,
				WorkerCount:      0,
				QueueSizePerLane: 1,
				LaneQuotas:       map[Lane]int{"default": 1},
			},
			wantErr: ErrInvalidWorkerCount,
		},
		{
			name: "zero queue size",
			config: Config{
				ShardCount:       1,
				WorkerCount:      1,
				QueueSizePerLane: 0,
				LaneQuotas:       map[Lane]int{"default": 1},
			},
			wantErr: ErrInvalidQueueSize,
		},
		{
			name: "nil lane quotas",
			config: Config{
				ShardCount:       1,
				WorkerCount:      1,
				QueueSizePerLane: 1,
				LaneQuotas:       nil,
			},
			wantErr: ErrMissingLaneQuotas,
		},
		{
			name: "empty lane name",
			config: Config{
				ShardCount:       1,
				WorkerCount:      1,
				QueueSizePerLane: 1,
				LaneQuotas:       map[Lane]int{"": 1},
			},
			wantErr: ErrInvalidLane,
		},
		{
			name: "zero quota",
			config: Config{
				ShardCount:       1,
				WorkerCount:      1,
				QueueSizePerLane: 1,
				LaneQuotas:       map[Lane]int{"default": 0},
			},
			wantErr: ErrInvalidLaneQuota,
		},
		{
			name: "negative shard count",
			config: Config{
				ShardCount:       -1,
				WorkerCount:      1,
				QueueSizePerLane: 1,
				LaneQuotas:       map[Lane]int{"default": 1},
			},
			wantErr: ErrInvalidShardCount,
		},
		{
			name: "negative worker count",
			config: Config{
				ShardCount:       1,
				WorkerCount:      -1,
				QueueSizePerLane: 1,
				LaneQuotas:       map[Lane]int{"default": 1},
			},
			wantErr: ErrInvalidWorkerCount,
		},
		{
			name: "negative queue size",
			config: Config{
				ShardCount:       1,
				WorkerCount:      1,
				QueueSizePerLane: -1,
				LaneQuotas:       map[Lane]int{"default": 1},
			},
			wantErr: ErrInvalidQueueSize,
		},
		{
			name: "empty lane quotas map",
			config: Config{
				ShardCount:       1,
				WorkerCount:      1,
				QueueSizePerLane: 1,
				LaneQuotas:       map[Lane]int{},
			},
			wantErr: ErrMissingLaneQuotas,
		},
		{
			name: "negative lane quota",
			config: Config{
				ShardCount:       1,
				WorkerCount:      1,
				QueueSizePerLane: 1,
				LaneQuotas:       map[Lane]int{"default": -1},
			},
			wantErr: ErrInvalidLaneQuota,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("Validate() error = %v, wantErr nil", err)
				}
			} else {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}
