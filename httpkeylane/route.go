// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"net/http"
	"strings"

	"github.com/haluan/go-keylane"
)

// LaneRule maps HTTP method and path prefix to a lane.
// Empty Method matches any method. Empty PathPrefix matches any path.
// Rules are evaluated in declared order; the first match wins.
// Put more specific rules before more general rules.
type LaneRule struct {
	Method     string
	PathPrefix string
	Lane       keylane.Lane
}

type normalizedLaneRule struct {
	method     string
	pathPrefix string
	lane       keylane.Lane
}

// RouteLaneMapper returns a LaneFunc that evaluates rules in order and returns
// the lane from the first matching rule. If no rule matches, fallback is called.
// A nil fallback returns an empty lane.
func RouteLaneMapper(rules []LaneRule, fallback LaneFunc) LaneFunc {
	copied := make([]normalizedLaneRule, len(rules))
	for i, rule := range rules {
		copied[i] = normalizedLaneRule{
			method:     strings.ToUpper(strings.TrimSpace(rule.Method)),
			pathPrefix: rule.PathPrefix,
			lane:       rule.Lane,
		}
	}
	return func(r *http.Request) keylane.Lane {
		method := strings.ToUpper(r.Method)
		path := r.URL.Path
		for _, rule := range copied {
			if rule.method != "" && rule.method != method {
				continue
			}
			if rule.pathPrefix != "" && !strings.HasPrefix(path, rule.pathPrefix) {
				continue
			}
			return rule.lane
		}
		if fallback == nil {
			return keylane.Lane("")
		}
		return fallback(r)
	}
}
