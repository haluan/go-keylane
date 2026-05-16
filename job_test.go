package keylane

import (
	"context"
	"errors"
	"testing"
)

func TestJob_Validate(t *testing.T) {
	tests := []struct {
		name    string
		job     Job
		wantErr error
	}{
		{
			name: "valid job",
			job: Job{
				Key:  "user-123",
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			},
			wantErr: nil,
		},
		{
			name: "empty key",
			job: Job{
				Key:  "",
				Lane: "default",
				Run:  func(ctx context.Context) error { return nil },
			},
			wantErr: ErrInvalidKey,
		},
		{
			name: "empty lane",
			job: Job{
				Key:  "user-123",
				Lane: "",
				Run:  func(ctx context.Context) error { return nil },
			},
			wantErr: ErrInvalidJob,
		},
		{
			name: "nil run function",
			job: Job{
				Key:  "user-123",
				Lane: "default",
				Run:  nil,
			},
			wantErr: ErrNilJobRun,
		},
		{
			name: "all invalid fields",
			job: Job{
				Key:  "",
				Lane: "",
				Run:  nil,
			},
			wantErr: ErrInvalidKey, // Validation stops at first error (Key)
		},
		{
			name: "empty lane - check error chain",
			job: Job{
				Key:  "user-123",
				Lane: "",
				Run:  func(ctx context.Context) error { return nil },
			},
			wantErr: ErrInvalidLane, // Should be checkable with errors.Is
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.job.Validate()
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
