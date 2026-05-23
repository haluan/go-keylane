// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"fmt"
	"time"
)

// BackoffHint provides retry/backoff guidance for callers and HTTP middleware.
// Keylane does not sleep, retry, or apply jitter internally.
type BackoffHint struct {
	RetryAfter time.Duration
	MinDelay   time.Duration
	MaxDelay   time.Duration
	Jitter     bool
}

// ValidateBackoffHint returns an error if durations are invalid.
func ValidateBackoffHint(h BackoffHint) error {
	if h.RetryAfter < 0 {
		return fmt.Errorf("%w: RetryAfter must be non-negative", ErrInvalidOverloadPolicy)
	}
	if h.MinDelay < 0 {
		return fmt.Errorf("%w: MinBackoff must be non-negative", ErrInvalidOverloadPolicy)
	}
	if h.MaxDelay < 0 {
		return fmt.Errorf("%w: MaxBackoff must be non-negative", ErrInvalidOverloadPolicy)
	}
	if h.MaxDelay > 0 && h.MinDelay > h.MaxDelay {
		return fmt.Errorf("%w: MaxBackoff must be >= MinBackoff", ErrInvalidOverloadPolicy)
	}
	if h.MaxDelay > 0 && h.RetryAfter > h.MaxDelay {
		return fmt.Errorf("%w: RetryAfter must not exceed MaxBackoff", ErrInvalidOverloadPolicy)
	}
	return nil
}

func backoffHintFromCore(retryAfter, minBackoff, maxBackoff time.Duration, jitter bool) BackoffHint {
	return BackoffHint{
		RetryAfter: retryAfter,
		MinDelay:   minBackoff,
		MaxDelay:   maxBackoff,
		Jitter:     jitter,
	}
}

// maxBackoffDuration is the upper bound for configured backoff durations.
const maxBackoffDuration = 24 * time.Hour

func validateBackoffWithinLimit(d time.Duration, name string) error {
	if d > maxBackoffDuration {
		return fmt.Errorf("%w: %s exceeds maximum %s", ErrInvalidOverloadPolicy, name, maxBackoffDuration)
	}
	return nil
}

func validateLaneOverloadBackoff(lp LaneOverloadPolicy) error {
	if err := validateBackoffWithinLimit(lp.RetryAfter, "RetryAfter"); err != nil {
		return err
	}
	if err := validateBackoffWithinLimit(lp.MinBackoff, "MinBackoff"); err != nil {
		return err
	}
	if err := validateBackoffWithinLimit(lp.MaxBackoff, "MaxBackoff"); err != nil {
		return err
	}
	return ValidateBackoffHint(BackoffHint{
		RetryAfter: lp.RetryAfter,
		MinDelay:   lp.MinBackoff,
		MaxDelay:   lp.MaxBackoff,
		Jitter:     lp.Jitter,
	})
}
