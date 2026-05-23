// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"fmt"
	"net/http"
	"strings"
)

func trimKey(s string) string {
	return strings.TrimSpace(s)
}

// HeaderKey returns a KeyFunc that reads the named request header.
// Leading and trailing whitespace are trimmed. Missing headers yield "".
func HeaderKey(name string) KeyFunc {
	return func(r *http.Request) string {
		return trimKey(r.Header.Get(name))
	}
}

// QueryKey returns a KeyFunc that reads the named URL query parameter.
// Leading and trailing whitespace are trimmed. Missing values yield "".
func QueryKey(name string) KeyFunc {
	return func(r *http.Request) string {
		return trimKey(r.URL.Query().Get(name))
	}
}

// PathValueKey returns a KeyFunc that reads a path value from the request
// using net/http's PathValue API. Missing values yield "".
func PathValueKey(name string) KeyFunc {
	return func(r *http.Request) string {
		return trimKey(r.PathValue(name))
	}
}

// CookieKey returns a KeyFunc that reads the named cookie value.
// Missing cookies yield "" without panicking.
func CookieKey(name string) KeyFunc {
	return func(r *http.Request) string {
		c, err := r.Cookie(name)
		if err != nil {
			return ""
		}
		return trimKey(c.Value)
	}
}

// StaticKey returns a KeyFunc that always returns the configured value (trimmed).
func StaticKey(value string) KeyFunc {
	v := trimKey(value)
	return func(*http.Request) string {
		return v
	}
}

// RemoteAddrKey returns a KeyFunc that uses r.RemoteAddr with whitespace trimmed.
//
// RemoteAddrKey is not recommended as a stable tenant key behind reverse proxies
// unless the server is configured to set RemoteAddr from a trusted header.
func RemoteAddrKey() KeyFunc {
	return func(r *http.Request) string {
		return strings.TrimSpace(r.RemoteAddr)
	}
}

// CompositeKey evaluates each KeyFunc in order, skips empty parts, and joins
// non-empty parts using length-prefixed segments separated by "|".
//
// Each segment is encoded as "<byte-length>:<value>". For example, parts
// "tenant-42" and "customer-9" produce "8:tenant-42|10:customer-9".
// This avoids collisions that naive delimiter joining can cause.
// If all parts are empty, CompositeKey returns "".
func CompositeKey(parts ...KeyFunc) KeyFunc {
	return func(r *http.Request) string {
		var b strings.Builder
		first := true
		for _, part := range parts {
			v := part(r)
			if v == "" {
				continue
			}
			if !first {
				b.WriteByte('|')
			}
			fmt.Fprintf(&b, "%d:%s", len(v), v)
			first = false
		}
		return b.String()
	}
}

// FirstNonEmptyKey evaluates each KeyFunc in order and returns the first non-empty key.
// If all parts are empty, it returns "".
func FirstNonEmptyKey(parts ...KeyFunc) KeyFunc {
	return func(r *http.Request) string {
		for _, part := range parts {
			if v := part(r); v != "" {
				return v
			}
		}
		return ""
	}
}
