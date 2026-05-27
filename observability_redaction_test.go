// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "testing"

func TestRedactRequestObservationDefault(t *testing.T) {
	obs := RequestObservation{RequestID: "rid", Key: "secret", Lane: "default"}
	got := redactRequestObservation(obs, false)
	if got.Key != "" || got.RequestID != "" {
		t.Fatalf("got = %+v, want redacted identifiers", got)
	}
	if got.KeyHash != HashKey("secret") {
		t.Fatalf("KeyHash = %d", got.KeyHash)
	}
}

func TestRedactRequestObservationExposeRaw(t *testing.T) {
	obs := RequestObservation{RequestID: "rid", Key: "secret", Lane: "default"}
	got := redactRequestObservation(obs, true)
	if got.Key != "secret" || got.RequestID != "rid" {
		t.Fatalf("got = %+v, want raw identifiers", got)
	}
}
