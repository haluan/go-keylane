// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/haluan/go-keylane"
)

// OverloadHTTPConfig configures HTTP responses for overload decisions.
type OverloadHTTPConfig struct {
	RejectStatusCode  int
	ShedStatusCode    int
	DegradeStatusCode int
	EnableRetryAfter  bool
}

// DegradeHandler runs a cheaper fallback when overload policy returns degrade.
type DegradeHandler func(w http.ResponseWriter, r *http.Request, decision keylane.OverloadDecision)

// OverloadConfig configures overload policy for HTTP middleware.
type OverloadConfig struct {
	Enabled bool
	HTTP    OverloadHTTPConfig
	// DegradeHandler is optional. When set, degrade decisions invoke this handler.
	DegradeHandler DegradeHandler
}

// NormalizeOverloadHTTPConfig applies defaults for overload HTTP mapping.
func NormalizeOverloadHTTPConfig(cfg *OverloadHTTPConfig) {
	if cfg == nil {
		return
	}
	if cfg.RejectStatusCode == 0 {
		cfg.RejectStatusCode = http.StatusServiceUnavailable
	}
	if cfg.ShedStatusCode == 0 {
		cfg.ShedStatusCode = http.StatusTooManyRequests
	}
	if cfg.DegradeStatusCode == 0 {
		cfg.DegradeStatusCode = http.StatusServiceUnavailable
	}
}

// ValidateOverloadConfig validates HTTP overload settings.
func ValidateOverloadConfig(cfg OverloadConfig) error {
	if !cfg.Enabled {
		return nil
	}
	normalized := cfg.HTTP
	NormalizeOverloadHTTPConfig(&normalized)
	for _, code := range []int{normalized.RejectStatusCode, normalized.ShedStatusCode, normalized.DegradeStatusCode} {
		if code < 100 || code > 599 {
			return fmt.Errorf("%w: overload HTTP status must be valid", keylane.ErrInvalidConfig)
		}
	}
	return nil
}

// CoreConfig returns the transport-agnostic overload config for keylane.CheckOverload.
func (c OverloadConfig) CoreConfig() keylane.OverloadConfig {
	return keylane.OverloadConfig{Enabled: c.Enabled}
}

func writeRetryAfter(w http.ResponseWriter, decision keylane.OverloadDecision, cfg OverloadHTTPConfig) {
	if !cfg.EnableRetryAfter || decision.RetryAfter <= 0 {
		return
	}
	secs := int(decision.RetryAfter.Seconds())
	if secs < 1 {
		secs = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(secs))
}

func overloadStatusCode(err error, cfg OverloadHTTPConfig, degradeHandler DegradeHandler) int {
	if err == nil {
		return 0
	}
	NormalizeOverloadHTTPConfig(&cfg)

	var oerr keylane.OverloadError
	if !errors.As(err, &oerr) {
		return 0
	}
	if !errors.Is(err, keylane.ErrOverloadRejected) &&
		!errors.Is(err, keylane.ErrOverloadShed) &&
		!errors.Is(err, keylane.ErrOverloadDegraded) {
		return 0
	}

	switch oerr.Decision.Action {
	case keylane.OverloadShed:
		if oerr.Decision.Reason == keylane.OverloadReasonLaneDepthExceeded {
			return http.StatusTooManyRequests
		}
		return cfg.ShedStatusCode
	case keylane.OverloadDegrade:
		if degradeHandler != nil {
			return 0
		}
		return cfg.DegradeStatusCode
	case keylane.OverloadReject:
		if oerr.Decision.Reason == keylane.OverloadReasonLaneDepthExceeded {
			return http.StatusTooManyRequests
		}
		return cfg.RejectStatusCode
	default:
		return cfg.RejectStatusCode
	}
}
