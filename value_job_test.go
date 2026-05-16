package keylane

import (
	"context"
	"errors"
	"testing"
)

func TestValueJobValidation(t *testing.T) {
	tests := []struct {
		name string
		job  ValueJob[int]
		want error
	}{
		{
			name: "valid job",
			job:  ValueJob[int]{Key: "k", Lane: "test", Run: func(ctx context.Context) (int, error) { return 0, nil }},
			want: nil,
		},
		{
			name: "empty key",
			job:  ValueJob[int]{Key: "", Lane: "test", Run: func(ctx context.Context) (int, error) { return 0, nil }},
			want: ErrInvalidKey,
		},
		{
			name: "empty lane",
			job:  ValueJob[int]{Key: "k", Lane: "", Run: func(ctx context.Context) (int, error) { return 0, nil }},
			want: ErrInvalidLane,
		},
		{
			name: "nil run",
			job:  ValueJob[int]{Key: "k", Lane: "test", Run: nil},
			want: ErrNilJobRun,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateValueJob(tt.job)
			if !errors.Is(err, tt.want) {
				t.Errorf("got error %v, want %v", err, tt.want)
			}
		})
	}
}
