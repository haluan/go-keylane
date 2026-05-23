// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// Package httpkeylane provides net/http middleware that executes handlers through
// the Keylane request runtime (SubmitRequest).
//
// The HTTP request context (r.Context()) is used for submission and awaiting.
// Client disconnects and upstream timeouts propagate as context cancellation.
// An Await timeout on a separate context only stops the caller from waiting; it
// does not cancel the underlying request unless that context is the same as the
// request context.
//
// KeyFunc and LaneFunc are required. There is no implicit default key or lane.
//
// Example:
//
//	mw := httpkeylane.Middleware(queue, httpkeylane.Config{
//	    KeyFunc: func(r *http.Request) string {
//	        return r.Header.Get("X-Tenant-ID")
//	    },
//	    LaneFunc: func(r *http.Request) keylane.Lane {
//	        return keylane.Lane(r.Method)
//	    },
//	})
//	http.Handle("/api/", mw(handler))
package httpkeylane
