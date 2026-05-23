// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/haluan/go-keylane"
)

func TestStaticLane(t *testing.T) {
	req := newTestRequest(t, http.MethodGet, "/")
	lane := keylane.Lane("custom")
	if got := StaticLane(lane)(req); got != lane {
		t.Errorf("StaticLane = %q, want %q", got, lane)
	}
}

func TestMethodLaneMapper(t *testing.T) {
	mapper := MethodLaneMapper()
	tests := []struct {
		method string
		want   keylane.Lane
	}{
		{http.MethodGet, LaneRead},
		{http.MethodHead, LaneRead},
		{http.MethodOptions, LaneRead},
		{http.MethodPost, LaneWrite},
		{http.MethodPut, LaneWrite},
		{http.MethodPatch, LaneWrite},
		{http.MethodDelete, LaneWrite},
		{"CUSTOM", LaneWrite},
	}
	for _, tt := range tests {
		req := newTestRequest(t, tt.method, "/")
		if got := mapper(req); got != tt.want {
			t.Errorf("MethodLaneMapper(%s) = %q, want %q", tt.method, got, tt.want)
		}
	}
}

func TestMethodLaneMapperWith(t *testing.T) {
	original := map[string]keylane.Lane{
		"get": keylane.Lane("custom-read"),
	}
	mapper := MethodLaneMapperWith(original, keylane.Lane("fallback"))

	req := newTestRequest(t, http.MethodGet, "/")
	if got := mapper(req); got != keylane.Lane("custom-read") {
		t.Errorf("GET = %q, want custom-read", got)
	}

	original["get"] = keylane.Lane("mutated")
	if got := mapper(req); got != keylane.Lane("custom-read") {
		t.Errorf("after map mutation = %q, want custom-read (copy)", got)
	}

	reqPost := newTestRequest(t, http.MethodPost, "/")
	if got := mapper(reqPost); got != keylane.Lane("fallback") {
		t.Errorf("POST = %q, want fallback", got)
	}

	// Case normalization at construction.
	mapper2 := MethodLaneMapperWith(map[string]keylane.Lane{
		"PoSt": keylane.Lane("post-lane"),
	}, keylane.Lane("fb"))
	reqPost2 := newTestRequest(t, http.MethodPost, "/")
	if got := mapper2(reqPost2); got != keylane.Lane("post-lane") {
		t.Errorf("normalized POST = %q, want post-lane", got)
	}
}

func ExampleMethodLaneMapper() {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	_ = MethodLaneMapper()(req)
}
