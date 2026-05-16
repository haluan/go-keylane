package core

import (
	"errors"
	"testing"
)

func TestNewLaneRegistry(t *testing.T) {
	tests := []struct {
		name    string
		quotas  map[string]int
		wantErr error
	}{
		{
			name: "valid registry",
			quotas: map[string]int{
				"default": 10,
				"high":    20,
			},
			wantErr: nil,
		},
		{
			name:    "nil quotas",
			quotas:  nil,
			wantErr: ErrMissingLaneQuotas,
		},
		{
			name:    "empty quotas",
			quotas:  map[string]int{},
			wantErr: ErrMissingLaneQuotas,
		},
		{
			name: "empty lane name",
			quotas: map[string]int{
				"": 10,
			},
			wantErr: ErrInvalidLane,
		},
		{
			name: "zero quota",
			quotas: map[string]int{
				"default": 0,
			},
			wantErr: ErrInvalidLaneQuota,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := NewLaneRegistry(tt.quotas)
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("NewLaneRegistry() error = %v, wantErr nil", err)
				}
				if r == nil {
					t.Fatal("NewLaneRegistry() returned nil registry")
				}
			} else {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("NewLaneRegistry() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}

func TestLaneRegistry_Methods(t *testing.T) {
	quotas := map[string]int{
		"alpha": 10,
		"beta":  20,
	}
	r, err := NewLaneRegistry(quotas)
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}

	if r.Len() != 2 {
		t.Errorf("Len() = %d, want 2", r.Len())
	}

	// Test Lookup
	idAlpha, ok := r.Lookup("alpha")
	if !ok {
		t.Error("Lookup(alpha) failed")
	}
	idBeta, ok := r.Lookup("beta")
	if !ok {
		t.Error("Lookup(beta) failed")
	}
	_, ok = r.Lookup("gamma")
	if ok {
		t.Error("Lookup(gamma) should have failed")
	}

	// Test Quota and Name
	if r.Quota(idAlpha) != 10 {
		t.Errorf("Quota(alpha) = %d, want 10", r.Quota(idAlpha))
	}
	if r.Name(idAlpha) != "alpha" {
		t.Errorf("Name(alpha) = %q, want \"alpha\"", r.Name(idAlpha))
	}

	if r.Quota(idBeta) != 20 {
		t.Errorf("Quota(beta) = %d, want 20", r.Quota(idBeta))
	}
	if r.Name(idBeta) != "beta" {
		t.Errorf("Name(beta) = %q, want \"beta\"", r.Name(idBeta))
	}

	// Test invalid ID
	if r.Quota(999) != 0 {
		t.Errorf("Quota(999) = %d, want 0", r.Quota(999))
	}
	if r.Name(999) != "" {
		t.Errorf("Name(999) = %q, want \"\"", r.Name(999))
	}
}
