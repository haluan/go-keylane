// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"time"
)

func (q *Queue) collectBackendPressure(ctx context.Context) []BackendPressureDiagnostic {
	if q == nil || len(q.backendPressureProvs) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	out := make([]BackendPressureDiagnostic, 0, len(q.backendPressureProvs))
	for _, p := range q.backendPressureProvs {
		snap, ok := q.collectProviderPressure(ctx, p)
		if !ok {
			continue
		}
		out = append(out, backendPressureDiagnosticFromSnapshot(snap))
		q.emitBackendPressure(snap)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (q *Queue) collectProviderPressure(ctx context.Context, p BackendPressureProvider) (BackendPressureSnapshot, bool) {
	var snap BackendPressureSnapshot
	ok := false
	func() {
		defer func() { _ = recover() }()
		snap = normalizeBackendPressureSnapshot(p.BackendPressure(ctx))
		ok = ValidateBackendPressureSnapshot(snap) == nil
	}()
	return snap, ok
}

func (q *Queue) emitBackendPressure(snap BackendPressureSnapshot) {
	if !q.backendHooksEnabled() {
		return
	}
	fn := q.config.Observability.Hooks.Backend.OnBackendPressure
	if fn == nil {
		return
	}
	callRequestHook(func() {
		fn(BackendPressureEvent{Time: time.Now(), Snapshot: snap})
	})
}
