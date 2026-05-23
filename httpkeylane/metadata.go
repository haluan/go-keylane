// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"net/http"

	"github.com/haluan/go-keylane"
)

// TransportHTTP is the RequestMeta.Transport value set by HTTP middleware.
const TransportHTTP = "http"

// OperationFunc returns a stable low-cardinality operation name for observability.
// If nil, RequestMeta.Operation remains empty (raw URL path is not used by default).
type OperationFunc func(*http.Request) string

// ObserveFunc is an optional HTTP-specific callback after a request finishes or is rejected.
// Configure keylane.Config.Observability.Hooks.Request for queue wait and run duration.
type ObserveFunc func(HTTPRequestMetadata, keylane.RequestObservation)

// HTTPRequestMetadata holds HTTP-specific fields for optional Observe callbacks.
// Path may contain high-cardinality segments; prefer Operation on RequestMeta for labels.
type HTTPRequestMetadata struct {
	Method     string
	Path       string
	StatusCode int
}
