// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestRequest(t *testing.T, method, target string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	return req
}

func TestHeaderKey(t *testing.T) {
	req := newTestRequest(t, http.MethodGet, "/")
	req.Header.Set("X-Tenant-ID", "  tenant-1  ")

	if got := HeaderKey("X-Tenant-ID")(req); got != "tenant-1" {
		t.Errorf("HeaderKey = %q, want tenant-1", got)
	}
	if got := HeaderKey("X-Missing")(req); got != "" {
		t.Errorf("missing header = %q, want empty", got)
	}
}

func TestQueryKey(t *testing.T) {
	req := newTestRequest(t, http.MethodGet, "/?customer_id=+cust-9+")

	if got := QueryKey("customer_id")(req); got != "cust-9" {
		t.Errorf("QueryKey = %q, want cust-9", got)
	}
	if got := QueryKey("missing")(req); got != "" {
		t.Errorf("missing query = %q, want empty", got)
	}
}

func TestPathValueKey(t *testing.T) {
	req := newTestRequest(t, http.MethodGet, "/tenants/42/")
	req.SetPathValue("tenant_id", "  42  ")

	if got := PathValueKey("tenant_id")(req); got != "42" {
		t.Errorf("PathValueKey = %q, want 42", got)
	}
	if got := PathValueKey("missing")(req); got != "" {
		t.Errorf("missing path value = %q, want empty", got)
	}
}

func TestCookieKey(t *testing.T) {
	req := newTestRequest(t, http.MethodGet, "/")
	req.AddCookie(&http.Cookie{Name: "session", Value: "  abc  "})

	if got := CookieKey("session")(req); got != "abc" {
		t.Errorf("CookieKey = %q, want abc", got)
	}
	if got := CookieKey("missing")(req); got != "" {
		t.Errorf("missing cookie = %q, want empty", got)
	}
}

func TestStaticKey(t *testing.T) {
	req := newTestRequest(t, http.MethodGet, "/")
	if got := StaticKey("  fixed  ")(req); got != "fixed" {
		t.Errorf("StaticKey = %q, want fixed", got)
	}
}

func TestRemoteAddrKey(t *testing.T) {
	req := newTestRequest(t, http.MethodGet, "/")
	req.RemoteAddr = "  127.0.0.1:1234  "
	if got := RemoteAddrKey()(req); got != "127.0.0.1:1234" {
		t.Errorf("RemoteAddrKey = %q, want 127.0.0.1:1234", got)
	}
}

func TestCompositeKey(t *testing.T) {
	req := newTestRequest(t, http.MethodGet, "/?customer_id=cust-9")
	req.Header.Set("X-Tenant-ID", "tenant-42")

	got := CompositeKey(
		HeaderKey("X-Tenant-ID"),
		QueryKey("customer_id"),
	)(req)
	want := "9:tenant-42|6:cust-9"
	if got != want {
		t.Errorf("CompositeKey = %q, want %q", got, want)
	}

	// Skips empty parts.
	req2 := newTestRequest(t, http.MethodGet, "/")
	got2 := CompositeKey(
		HeaderKey("X-Tenant-ID"),
		StaticKey("only"),
	)(req2)
	if got2 != "4:only" {
		t.Errorf("CompositeKey skip empty = %q, want 4:only", got2)
	}

	// All empty.
	if got := CompositeKey(HeaderKey("X"))(req2); got != "" {
		t.Errorf("all empty = %q, want empty", got)
	}

	// Collision resistance: "a"+"bc" vs "ab"+"c" differ.
	req3 := newTestRequest(t, http.MethodGet, "/")
	key1 := CompositeKey(StaticKey("a"), StaticKey("bc"))(req3)
	key2 := CompositeKey(StaticKey("ab"), StaticKey("c"))(req3)
	if key1 == key2 {
		t.Errorf("collision: both keys = %q", key1)
	}
	if key1 != "1:a|2:bc" {
		t.Errorf("key1 = %q, want 1:a|2:bc", key1)
	}
	if key2 != "2:ab|1:c" {
		t.Errorf("key2 = %q, want 2:ab|1:c", key2)
	}
}

func TestFirstNonEmptyKey(t *testing.T) {
	req := newTestRequest(t, http.MethodGet, "/?tenant_id=from-query")

	got := FirstNonEmptyKey(
		HeaderKey("X-Tenant-ID"),
		QueryKey("tenant_id"),
	)(req)
	if got != "from-query" {
		t.Errorf("FirstNonEmptyKey = %q, want from-query", got)
	}

	req.Header.Set("X-Tenant-ID", "from-header")
	if got := FirstNonEmptyKey(
		HeaderKey("X-Tenant-ID"),
		QueryKey("tenant_id"),
	)(req); got != "from-header" {
		t.Errorf("FirstNonEmptyKey = %q, want from-header", got)
	}

	req2 := newTestRequest(t, http.MethodGet, "/")
	if got := FirstNonEmptyKey(HeaderKey("X"))(req2); got != "" {
		t.Errorf("all empty = %q, want empty", got)
	}
}

func ExampleHeaderKey() {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Tenant-ID", "tenant-1")
	// Output is consumed by middleware KeyFunc; not printed here.
	_ = HeaderKey("X-Tenant-ID")(req)
}

func ExampleCompositeKey() {
	req := httptest.NewRequest(http.MethodGet, "/?customer_id=cust-9", nil)
	req.Header.Set("X-Tenant-ID", "tenant-42")
	_ = CompositeKey(HeaderKey("X-Tenant-ID"), QueryKey("customer_id"))(req)
}
