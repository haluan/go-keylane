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
// Use key helpers (HeaderKey, CompositeKey, FirstNonEmptyKey, etc.) and lane
// helpers (MethodLaneMapper, RouteLaneMapper) to reduce boilerplate.
//
// CompositeKey joins non-empty parts as length-prefixed segments separated by "|"
// (for example "8:tenant-42|10:customer-9") to avoid delimiter collisions.
//
// RouteLaneMapper evaluates LaneRule entries in declared order; the first match
// wins. Put more specific rules before general rules.
//
// Admission control (Config.Admission) is disabled by default. When enabled, requests
// are rejected before enqueue when Queue.Pressure().TotalDepthRatio meets or exceeds
// RejectAboveRatio (default 0.90). Default HTTP rejection status is 503.
//
// Observability: set OperationFunc for a stable operation label on RequestMeta.Operation.
// Optional Config.Observe receives HTTPRequestMetadata (method, path, status) plus a
// keylane.RequestObservation snapshot. For queue wait and run duration, configure
// keylane.Config.Observability.Hooks.Request on the queue.
//
// Example:
//
//	mw := httpkeylane.Middleware(queue, httpkeylane.Config{
//	    KeyFunc: httpkeylane.FirstNonEmptyKey(
//	        httpkeylane.HeaderKey("X-Tenant-ID"),
//	        httpkeylane.QueryKey("tenant_id"),
//	    ),
//	    LaneFunc: httpkeylane.RouteLaneMapper(
//	        []httpkeylane.LaneRule{
//	            {Method: http.MethodPost, PathPrefix: "/payments", Lane: keylane.Lane("payment-write")},
//	        },
//	        httpkeylane.MethodLaneMapper(),
//	    ),
//	})
//	http.Handle("/api/", mw(handler))
package httpkeylane
