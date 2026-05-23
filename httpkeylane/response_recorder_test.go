// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResponseRecorderDefault200OnWrite(t *testing.T) {
	rr := httptest.NewRecorder()
	rec := newResponseRecorder(rr)
	_, _ = rec.Write([]byte("ok"))
	if got := rec.StatusCode(); got != http.StatusOK {
		t.Errorf("StatusCode = %d, want 200", got)
	}
}

func TestResponseRecorderWriteHeader(t *testing.T) {
	rr := httptest.NewRecorder()
	rec := newResponseRecorder(rr)
	rec.WriteHeader(http.StatusTeapot)
	if got := rec.StatusCode(); got != http.StatusTeapot {
		t.Errorf("StatusCode = %d, want 418", got)
	}
}
