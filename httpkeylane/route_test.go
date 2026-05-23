// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"net/http"
	"testing"

	"github.com/haluan/go-keylane"
)

func TestRouteLaneMapperFirstMatchWins(t *testing.T) {
	generalFirst := []LaneRule{
		{Method: http.MethodPost, PathPrefix: "/payments", Lane: keylane.Lane("payment-write")},
		{Method: http.MethodPost, PathPrefix: "/payments/refunds", Lane: keylane.Lane("refund-write")},
	}
	mapper := RouteLaneMapper(generalFirst, nil)
	req := newTestRequest(t, http.MethodPost, "/payments/refunds")
	if got := mapper(req); got != keylane.Lane("payment-write") {
		t.Errorf("general first = %q, want payment-write", got)
	}

	specificFirst := []LaneRule{
		{Method: http.MethodPost, PathPrefix: "/payments/refunds", Lane: keylane.Lane("refund-write")},
		{Method: http.MethodPost, PathPrefix: "/payments", Lane: keylane.Lane("payment-write")},
	}
	mapper2 := RouteLaneMapper(specificFirst, nil)
	if got := mapper2(req); got != keylane.Lane("refund-write") {
		t.Errorf("specific first = %q, want refund-write", got)
	}
}

func TestRouteLaneMapperMethodMatch(t *testing.T) {
	rules := []LaneRule{
		{Method: http.MethodPost, PathPrefix: "/payments", Lane: keylane.Lane("payment-write")},
	}
	mapper := RouteLaneMapper(rules, nil)

	getReq := newTestRequest(t, http.MethodGet, "/payments")
	if got := mapper(getReq); got != keylane.Lane("") {
		t.Errorf("GET /payments = %q, want empty (no fallback)", got)
	}
}

func TestRouteLaneMapperAnyMethod(t *testing.T) {
	rules := []LaneRule{
		{Method: "", PathPrefix: "/health", Lane: keylane.Lane("health")},
	}
	mapper := RouteLaneMapper(rules, nil)

	for _, method := range []string{http.MethodGet, http.MethodPost, "PATCH"} {
		req := newTestRequest(t, method, "/healthz")
		if got := mapper(req); got != keylane.Lane("health") {
			t.Errorf("%s /healthz = %q, want health", method, got)
		}
	}
}

func TestRouteLaneMapperAnyPath(t *testing.T) {
	rules := []LaneRule{
		{Method: http.MethodGet, PathPrefix: "", Lane: keylane.Lane("read-all")},
	}
	mapper := RouteLaneMapper(rules, nil)
	req := newTestRequest(t, http.MethodGet, "/anything")
	if got := mapper(req); got != keylane.Lane("read-all") {
		t.Errorf("any path = %q, want read-all", got)
	}
}

func TestRouteLaneMapperFallback(t *testing.T) {
	rules := []LaneRule{
		{Method: http.MethodPost, PathPrefix: "/payments", Lane: keylane.Lane("payment-write")},
	}
	fallback := MethodLaneMapper()
	mapper := RouteLaneMapper(rules, fallback)

	req := newTestRequest(t, http.MethodGet, "/reports")
	if got := mapper(req); got != LaneRead {
		t.Errorf("fallback = %q, want read", got)
	}

	nilFallback := RouteLaneMapper(rules, nil)
	if got := nilFallback(req); got != keylane.Lane("") {
		t.Errorf("nil fallback = %q, want empty", got)
	}
}

func TestRouteLaneMapperRuleCopy(t *testing.T) {
	rules := []LaneRule{
		{Method: http.MethodGet, PathPrefix: "/a", Lane: keylane.Lane("a-lane")},
	}
	mapper := RouteLaneMapper(rules, nil)
	rules[0].Lane = keylane.Lane("mutated")

	req := newTestRequest(t, http.MethodGet, "/a")
	if got := mapper(req); got != keylane.Lane("a-lane") {
		t.Errorf("after slice mutation = %q, want a-lane", got)
	}
}

func ExampleRouteLaneMapper() {
	rules := []LaneRule{
		{Method: http.MethodGet, PathPrefix: "/reports", Lane: keylane.Lane("report-read")},
	}
	_ = RouteLaneMapper(rules, MethodLaneMapper())
}
