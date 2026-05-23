// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import "github.com/haluan/go-keylane/internal/core"

// ShardIDForKey returns the shard index for key using the same routing as enqueue.
func (q *Queue) ShardIDForKey(key string) int {
	if q == nil || q.config.ShardCount < 1 {
		return 0
	}
	return int(core.HashKey(key) % uint64(q.config.ShardCount))
}

func (q *Queue) requestHooksEnabled() bool {
	return q.config.Observability.EnableHooks
}

func (q *Queue) requestHooksNeedWorkerTiming() bool {
	if !q.requestHooksEnabled() {
		return false
	}
	h := q.config.Observability.Hooks.Request
	return h.OnStarted != nil || h.OnCompleted != nil
}

func (q *Queue) emitRequestQueued(meta RequestMeta) {
	if !q.requestHooksEnabled() {
		return
	}
	fn := q.config.Observability.Hooks.Request.OnQueued
	if fn == nil {
		return
	}
	callRequestHook(func() { fn(meta) })
}

func (q *Queue) emitRequestStarted(obs RequestObservation) {
	if !q.requestHooksEnabled() {
		return
	}
	fn := q.config.Observability.Hooks.Request.OnStarted
	if fn == nil {
		return
	}
	callRequestHook(func() { fn(obs) })
}

func (q *Queue) emitRequestCompleted(obs RequestObservation) {
	if !q.requestHooksEnabled() {
		return
	}
	fn := q.config.Observability.Hooks.Request.OnCompleted
	if fn == nil {
		return
	}
	callRequestHook(func() { fn(obs) })
}

func (q *Queue) emitRequestRejected(obs RequestObservation) {
	if !q.requestHooksEnabled() {
		return
	}
	fn := q.config.Observability.Hooks.Request.OnRejected
	if fn == nil {
		return
	}
	callRequestHook(func() { fn(obs) })
}

func callRequestHook(fn func()) {
	defer func() { _ = recover() }()
	fn()
}
