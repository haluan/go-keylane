// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
)

// Stop gracefully stops the queue processing.
func (q *Queue) Stop(ctx context.Context, opts ...StopOption) error {
	cfg := stopConfig{
		drain: true,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if q.adaptive != nil {
		q.adaptive.Stop()
	}
	return q.sched.Stop(ctx, cfg.drain)
}
