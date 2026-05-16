package keylane

import (
	"errors"
	"testing"
)

func TestLane_Validate(t *testing.T) {
	tests := []struct {
		name    string
		lane    Lane
		wantErr error
	}{
		{
			name:    "valid lane",
			lane:    "default",
			wantErr: nil,
		},
		{
			name:    "empty lane",
			lane:    "",
			wantErr: ErrInvalidLane,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.lane.Validate()
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
