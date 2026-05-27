// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

func (q *Queue) hookExposeRawIdentifiers() bool {
	return q != nil && q.config.Observability.ExposeRawRequestIdentifiers
}

func (q *Queue) redactRequestMeta(meta RequestMeta) RequestMeta {
	if q == nil || q.hookExposeRawIdentifiers() {
		return meta
	}
	meta.RequestID = ""
	meta.Key = ""
	return meta
}

func redactRequestObservation(obs RequestObservation, exposeRaw bool) RequestObservation {
	if obs.Key != "" {
		obs.KeyHash = HashKey(obs.Key)
	}
	if exposeRaw {
		return obs
	}
	obs.RequestID = ""
	obs.Key = ""
	return obs
}

func (q *Queue) redactRequestObservation(obs RequestObservation) RequestObservation {
	expose := q != nil && q.hookExposeRawIdentifiers()
	return redactRequestObservation(obs, expose)
}

func (q *Queue) redactContinuationObservation(obs ContinuationObservation) ContinuationObservation {
	if obs.Key != "" {
		obs.KeyHash = HashKey(obs.Key)
	}
	if q == nil || q.hookExposeRawIdentifiers() {
		return obs
	}
	obs.RequestID = ""
	obs.Key = ""
	return obs
}

func redactStageExecution(exec StageExecutionContext, exposeRaw bool) StageExecutionContext {
	if exposeRaw {
		return exec
	}
	exec.RequestID = ""
	exec.Key = ""
	return exec
}

func (q *Queue) redactStageObservation(obs StageObservation) StageObservation {
	if obs.Key != "" {
		obs.KeyHash = HashKey(obs.Key)
	}
	expose := q != nil && q.hookExposeRawIdentifiers()
	obs.Execution = redactStageExecution(obs.Execution, expose)
	if expose {
		return obs
	}
	obs.RequestID = ""
	obs.Key = ""
	return obs
}

func (q *Queue) redactRetryEvent(ev RetryEvent) RetryEvent {
	if ev.Key != "" {
		ev.KeyHash = HashKey(ev.Key)
	}
	if q == nil || q.hookExposeRawIdentifiers() {
		return ev
	}
	ev.Key = ""
	return ev
}

func (q *Queue) redactBackendAdmissionDecision(dec BackendAdmissionDecision) BackendAdmissionDecision {
	if q == nil || q.hookExposeRawIdentifiers() {
		return dec
	}
	dec.RequestID = ""
	return dec
}

func (q *Queue) redactBackendReleaseEvent(ev BackendReleaseEvent) BackendReleaseEvent {
	if q == nil || q.hookExposeRawIdentifiers() {
		return ev
	}
	ev.RequestID = ""
	return ev
}
