package core

import (
	"context"
	"errors"
	"testing"

	"github.com/haluan/go-keylane"
)

func TestNewInternalJob(t *testing.T) {
	runFn := func(ctx context.Context) error { return nil }
	validJob := keylane.Job{
		Key:  "test-key",
		Lane: "default",
		Run:  runFn,
	}

	tests := []struct {
		name     string
		job      keylane.Job
		keyHash  uint64
		laneID   LaneID
		wantErr  error
	}{
		{
			name:    "valid conversion",
			job:     validJob,
			keyHash: 12345,
			laneID:  1,
			wantErr: nil,
		},
		{
			name: "invalid public job",
			job: keylane.Job{
				Key: "", // invalid
			},
			wantErr: keylane.ErrInvalidKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ij, err := NewInternalJob(tt.job, tt.keyHash, tt.laneID)
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
				// We can't easily compare function pointers in Go, but we can check if it's nil/not nil
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
