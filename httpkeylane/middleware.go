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
	// Admission is optional pressure-based admission control (disabled by default).
	Admission AdmissionConfig
	// OperationFunc is optional. Sets RequestMeta.Operation when non-nil.
	OperationFunc OperationFunc
	// Observe is optional. Called with HTTP metadata and a request observation snapshot.
	Observe ObserveFunc
}

// DefaultErrorHandler maps errors to HTTP status codes and writes a plain-text body.
func DefaultErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	DefaultErrorHandlerWithAdmission(AdmissionConfig{})(w, r, err)
}

// DefaultErrorHandlerWithAdmission is the default error handler with admission status mapping.
func DefaultErrorHandlerWithAdmission(admission AdmissionConfig) ErrorHandler {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		status := statusCodeForError(err, admission)
		if status == 0 {
			return
		}
		http.Error(w, http.StatusText(status), status)
	}
}

// Middleware returns net/http middleware that runs the wrapped handler inside Keylane.
func Middleware(queue *keylane.Queue, cfg Config) func(http.Handler) http.Handler {
	admissionCfg := cfg.Admission
	NormalizeAdmissionConfig(&admissionCfg)

	eh := cfg.ErrorHandler
	if eh == nil {
		eh = DefaultErrorHandlerWithAdmission(admissionCfg)
	}

	if err := validateMiddlewareConfig(queue, cfg, admissionCfg); err != nil {
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
			rec := newResponseRecorder(w)

			meta := buildRequestMeta(cfg, r)
			if meta.Key == "" {
				eh(rec, r, keylane.ErrInvalidKey)
				cfg.observeHTTPError(r, rec, queue, meta, keylane.ErrInvalidKey)
				return
			}
			if err := meta.Lane.Validate(); err != nil {
				eh(rec, r, err)
				cfg.observeHTTPError(r, rec, queue, meta, err)
				return
			}

			if err := keylane.CheckAdmission(queue, admissionCfg.CoreConfig(), meta); err != nil {
				eh(rec, r, err)
				cfg.observeHTTPError(r, rec, queue, meta, err)
				return
			}

			var served atomic.Bool

			req := keylane.Request[struct{}, struct{}]{
				Meta:  meta,
				Input: struct{}{},
				Handle: func(reqCtx context.Context, _ struct{}) (struct{}, error) {
					if err := reqCtx.Err(); err != nil {
						return struct{}{}, err
					}
					served.Store(true)
					next.ServeHTTP(rec, r.WithContext(reqCtx))
					if err := reqCtx.Err(); err != nil {
						return struct{}{}, err
					}
					return struct{}{}, nil
				},
			}

			future, err := keylane.SubmitRequest(ctx, queue, req)
			if err != nil {
				eh(rec, r, err)
				cfg.observeHTTPError(r, rec, queue, meta, err)
				return
			}

			_, awaitErr := future.Await(ctx)
			if awaitErr != nil && !served.Load() {
				eh(rec, r, awaitErr)
			}
			cfg.observeHTTPError(r, rec, queue, meta, awaitErr)
		})
	}
}

func buildRequestMeta(cfg Config, r *http.Request) keylane.RequestMeta {
	meta := keylane.RequestMeta{
		Transport: TransportHTTP,
		Key:       cfg.KeyFunc(r),
		Lane:      cfg.LaneFunc(r),
	}
	if cfg.OperationFunc != nil {
		meta.Operation = cfg.OperationFunc(r)
	}
	if rid := r.Header.Get("X-Request-ID"); rid != "" {
		meta.RequestID = rid
	}
	return meta
}

func (cfg Config) observeHTTP(r *http.Request, rec *responseRecorder, obs keylane.RequestObservation) {
	if cfg.Observe == nil {
		return
	}
	cfg.Observe(HTTPRequestMetadata{
		Method:     r.Method,
		Path:       r.URL.Path,
		StatusCode: rec.StatusCode(),
	}, obs)
}

func (cfg Config) observeHTTPError(r *http.Request, rec *responseRecorder, q *keylane.Queue, meta keylane.RequestMeta, err error) {
	cfg.observeHTTP(r, rec, keylane.ObservationForError(q, meta, err))
}

func validateMiddlewareConfig(queue *keylane.Queue, cfg Config, admission AdmissionConfig) error {
	if queue == nil {
		return keylane.ErrNilQueue
	}
	if cfg.KeyFunc == nil {
		return ErrMissingKeyFunc
	}
	if cfg.LaneFunc == nil {
		return ErrMissingLaneFunc
	}
	return ValidateAdmissionConfig(admission)
}

func statusCodeForError(err error, admission AdmissionConfig) int {
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
	case errors.Is(err, keylane.ErrAdmissionRejected):
		var rej keylane.AdmissionRejectedError
		if errors.As(err, &rej) && rej.Reason == keylane.AdmissionReasonLaneQueueDepthExceeded {
			return http.StatusTooManyRequests
		}
		return rejectStatusCode(admission)
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
