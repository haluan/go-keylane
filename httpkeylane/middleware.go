// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"

	"github.com/haluan/go-keylane"
)

// KeyFunc extracts the shard routing key from an HTTP request.
type KeyFunc func(*http.Request) string

// LaneFunc extracts the lane from an HTTP request.
type LaneFunc func(*http.Request) keylane.Lane

// ErrorHandler writes an HTTP error response for middleware failures.
type ErrorHandler func(http.ResponseWriter, *http.Request, error)

// Config configures HTTP middleware.
type Config struct {
	// KeyFunc is required. Must return a non-empty key for each request.
	KeyFunc KeyFunc
	// LaneFunc is required. Must return a valid lane for each request.
	LaneFunc LaneFunc
	// ErrorHandler is optional. Defaults to status mapping via statusCodeForError.
	ErrorHandler ErrorHandler
}

// DefaultErrorHandler maps errors to HTTP status codes and writes a plain-text body.
func DefaultErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	status := statusCodeForError(err)
	if status == 0 {
		return
	}
	http.Error(w, http.StatusText(status), status)
}

// Middleware returns net/http middleware that runs the wrapped handler inside Keylane.
func Middleware(queue *keylane.Queue, cfg Config) func(http.Handler) http.Handler {
	eh := cfg.ErrorHandler
	if eh == nil {
		eh = DefaultErrorHandler
	}

	if err := validateMiddlewareConfig(queue, cfg); err != nil {
		configErr := err
		return func(http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				eh(w, r, configErr)
			})
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			key := cfg.KeyFunc(r)
			if key == "" {
				eh(w, r, keylane.ErrInvalidKey)
				return
			}

			lane := cfg.LaneFunc(r)
			if err := lane.Validate(); err != nil {
				eh(w, r, err)
				return
			}

			var served atomic.Bool
			meta := keylane.RequestMeta{
				Key:  key,
				Lane: lane,
			}
			if rid := r.Header.Get("X-Request-ID"); rid != "" {
				meta.RequestID = rid
			}

			req := keylane.Request[struct{}, struct{}]{
				Meta:  meta,
				Input: struct{}{},
				Handle: func(reqCtx context.Context, _ struct{}) (struct{}, error) {
					if err := reqCtx.Err(); err != nil {
						return struct{}{}, err
					}
					served.Store(true)
					next.ServeHTTP(w, r.WithContext(reqCtx))
					return struct{}{}, nil
				},
			}

			future, err := keylane.SubmitRequest(ctx, queue, req)
			if err != nil {
				eh(w, r, err)
				return
			}

			if _, err := future.Await(ctx); err != nil {
				if !served.Load() {
					eh(w, r, err)
				}
				return
			}
		})
	}
}

func validateMiddlewareConfig(queue *keylane.Queue, cfg Config) error {
	if queue == nil {
		return keylane.ErrNilQueue
	}
	if cfg.KeyFunc == nil {
		return ErrMissingKeyFunc
	}
	if cfg.LaneFunc == nil {
		return ErrMissingLaneFunc
	}
	return nil
}

func statusCodeForError(err error) int {
	if err == nil {
		return 0
	}
	switch {
	case errors.Is(err, ErrMissingKeyFunc),
		errors.Is(err, ErrMissingLaneFunc),
		errors.Is(err, keylane.ErrNilQueue):
		return http.StatusInternalServerError
	case errors.Is(err, keylane.ErrInvalidKey),
		errors.Is(err, keylane.ErrInvalidLane):
		return http.StatusBadRequest
	case errors.Is(err, keylane.ErrQueueFull),
		errors.Is(err, keylane.ErrStopped),
		errors.Is(err, keylane.ErrNotStarted),
		errors.Is(err, keylane.ErrQueueNotStarted):
		return http.StatusServiceUnavailable
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		return 499 // Client Closed Request (non-standard; not in older net/http constants)
	default:
		return http.StatusInternalServerError
	}
}
