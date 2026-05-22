// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"os/exec"
	"strings"
	"testing"
)

// TestDependencyBoundaryCoreModule verifies the root keylane module does not depend on
// Prometheus or OpenTelemetry (KL-1208). Adapters live in separate submodules.
func TestDependencyBoundaryCoreModule(t *testing.T) {
	t.Helper()
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("go list -deps: %v", err)
	}
	deps := string(out)
	for _, forbidden := range []string{
		"github.com/prometheus",
		"go.opentelemetry.io",
	} {
		if strings.Contains(deps, forbidden) {
			t.Errorf("core module must not depend on %s; found in go list -deps output", forbidden)
		}
	}
}
