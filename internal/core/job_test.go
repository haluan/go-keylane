package core

import (
	"context"
	"errors"
	"testing"
)

func TestNewInternalJob(t *testing.T) {
	runFn := func(ctx context.Context) error { return nil }

	tests := []struct {
		name    string
		run     func(context.Context) error
		keyHash uint64
		laneID  LaneID
		wantErr error
	}{
		{
			name:    "valid conversion",
			run:     runFn,
			keyHash: 12345,
			laneID:  1,
			wantErr: nil,
		},
		{
			name:    "nil run function",
			run:     nil,
			wantErr: ErrNilJobRun,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ij, err := NewInternalJob(tt.run, tt.keyHash, tt.laneID)
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("NewInternalJob() error = %v, wantErr nil", err)
				}
				if ij.KeyHash != tt.keyHash {
					t.Errorf("KeyHash = %d, want %d", ij.KeyHash, tt.keyHash)
				}
				if ij.LaneID != tt.laneID {
					t.Errorf("LaneID = %d, want %d", ij.LaneID, tt.laneID)
				}
				if ij.Run == nil {
					t.Error("Run function is nil")
				}
			} else {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("NewInternalJob() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}
