// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"fmt"
	"strings"
)

// BackendResourceName identifies a configured downstream resource (low cardinality).
type BackendResourceName string

// BackendLane classifies downstream operation class for a resource.
type BackendLane string

const (
	BackendLaneDBRead      BackendLane = "db_read"
	BackendLaneDBWrite     BackendLane = "db_write"
	BackendLaneExternalAPI BackendLane = "external_api"
	BackendLaneCacheRead   BackendLane = "cache_read"
	BackendLaneCacheWrite  BackendLane = "cache_write"
)

// BackendOperation describes a backend resource acquisition attempt.
type BackendOperation struct {
	Resource  BackendResourceName
	Lane      BackendLane
	Stage     StageName
	Operation string
}

// ValidateBackendOperation checks required fields and rejects obvious high-cardinality values.
func ValidateBackendOperation(op BackendOperation) error {
	if op.Resource == "" {
		return fmt.Errorf("%w: backend resource name is required", ErrInvalidConfig)
	}
	if op.Lane == "" {
		return fmt.Errorf("%w: backend lane is required", ErrInvalidConfig)
	}
	if err := validateBackendLabel(string(op.Resource), "resource"); err != nil {
		return err
	}
	if err := validateBackendLabel(string(op.Lane), "lane"); err != nil {
		return err
	}
	if op.Operation != "" {
		if err := validateBackendLabel(op.Operation, "operation"); err != nil {
			return err
		}
	}
	if op.Stage != "" {
		if err := validateBackendLabel(string(op.Stage), "stage"); err != nil {
			return err
		}
	}
	return nil
}

func validateBackendLabel(value, field string) error {
	if strings.Contains(value, "://") {
		return fmt.Errorf("%w: backend %s must not contain URL-like values", ErrInvalidConfig, field)
	}
	if len(value) > 128 {
		return fmt.Errorf("%w: backend %s exceeds max length 128", ErrInvalidConfig, field)
	}
	return nil
}
