// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"sync"
	"time"
)

// ContinuationID uniquely identifies a pending pipeline continuation.
type ContinuationID uint64

// ContinuationOutcomeKind classifies how a continuation resolved.
type ContinuationOutcomeKind int

const (
	continuationOutcomeComplete ContinuationOutcomeKind = iota
	continuationOutcomeFail
	continuationOutcomeCancel
)

// continuationOutcome carries the resolved state from a completer to the registry/runner.
type continuationOutcome[S any] struct {
	kind  ContinuationOutcomeKind
	state S
	err   error
}

// Continuation is an opaque handle returned by a yielding stage.
// It carries resume position and execution identity for the pipeline runner.
// The type parameter S is the pipeline state type.
type Continuation[S any] struct {
	ID         ContinuationID
	exec       StageExecutionContext
	stageIndex int
	stageCount int
	yieldedAt  time.Time

	// reqCtx is the pipeline request context; set by the runner when the continuation is registered.
	reqCtx context.Context
	reqMu  sync.Mutex

	// done is closed by the registry on any resolution (complete, fail, cancel).
	done chan struct{}
	// outcome receives exactly one value when the continuation resolves.
	outcome chan continuationOutcome[S]

	// lateOnce ensures at most one late-completion diagnostic per continuation (resolver vs completer races).
	lateOnce sync.Once

	boundCompleter ContinuationCompleter[S]
}

// recordLateCompletion increments late-completion diagnostics at most once for this continuation.
func (cont *Continuation[S]) recordLateCompletion(q *Queue, exec StageExecutionContext, kind ContinuationOutcomeKind, err error) {
	if kind == continuationOutcomeCancel {
		return
	}
	cont.lateOnce.Do(func() {
		q.continuationReg.recordLate()
		if err == nil {
			err = ErrContinuationLate
		}
		lateObs := continuationObsFromExec(
			cont.ID, exec, time.Since(cont.yieldedAt), 0,
			classifyOutcomeRequestOutcome(kind),
			classifyOutcomeFailureKind(kind),
			err,
		)
		q.emitContinuationLate(lateObs)
	})
}

// StageResult is returned by a ContinuationStageFunc.
// If Continuation is non-nil the stage has yielded; State is ignored.
// If Continuation is nil the stage completed synchronously and State carries the updated value.
// error != nil always signals immediate stage failure; Continuation must be nil.
type StageResult[S any] struct {
	State        S
	Continuation *Continuation[S]
}

// ContinuationStageFunc is a pipeline stage function that can yield execution.
type ContinuationStageFunc[S any] func(context.Context, S) (StageResult[S], error)

// ContinuationCompleter allows external code to resolve a pending continuation exactly once.
type ContinuationCompleter[S any] interface {
	// Complete advances the pipeline with the given updated state. Returns false if already resolved.
	Complete(state S) bool
	// Fail terminates the pipeline with the given error. Returns false if already resolved.
	Fail(err error) bool
	// Cancel terminates the pipeline with a cancellation error. Returns false if already resolved.
	Cancel(err error) bool
}

func (cont *Continuation[S]) setRequestContext(ctx context.Context) {
	cont.reqMu.Lock()
	cont.reqCtx = ctx
	cont.reqMu.Unlock()
}

func (cont *Continuation[S]) requestContextErr() error {
	cont.reqMu.Lock()
	ctx := cont.reqCtx
	cont.reqMu.Unlock()
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

type continuationCompleter[S any] struct {
	once sync.Once
	mu   sync.Mutex
	cont *Continuation[S]
	late func()
}

func (c *continuationCompleter[S]) invokeLate() {
	c.mu.Lock()
	late := c.late
	c.mu.Unlock()
	if late != nil {
		late()
	}
}

func (c *continuationCompleter[S]) resolveLate() bool {
	select {
	case <-c.cont.done:
		c.invokeLate()
		return true
	default:
		return false
	}
}

// deliver sends the outcome to the resolution goroutine without blocking. When the
// outcome channel is already full, the late handler runs and deliver returns false.
func (c *continuationCompleter[S]) deliver(o continuationOutcome[S]) bool {
	if c.resolveLate() {
		return false
	}
	if err := c.cont.requestContextErr(); err != nil {
		c.once.Do(func() { c.invokeLate() })
		return false
	}
	resolved := false
	c.once.Do(func() {
		select {
		case c.cont.outcome <- o:
			resolved = true
		default:
			c.invokeLate()
		}
	})
	// Once published, the resolver owns resolution (including late accounting when the
	// request is already cancelled). A trailing context check must not invoke the late
	// handler: cancellation may arrive after the resolver accepted the outcome.
	return resolved
}

func (c *continuationCompleter[S]) Complete(state S) bool {
	return c.deliver(continuationOutcome[S]{kind: continuationOutcomeComplete, state: state})
}

func (c *continuationCompleter[S]) Fail(err error) bool {
	return c.deliver(continuationOutcome[S]{kind: continuationOutcomeFail, err: err})
}

func (c *continuationCompleter[S]) Cancel(err error) bool {
	if c.resolveLate() {
		return false
	}
	errToSend := err
	if errToSend == nil {
		errToSend = ErrContinuationCancelled
	}
	return c.deliver(continuationOutcome[S]{kind: continuationOutcomeCancel, err: errToSend})
}

// setContinuationLateHandler registers a callback when Complete/Fail/Cancel runs after resolution.
func setContinuationLateHandler[S any](c ContinuationCompleter[S], late func()) {
	if cc, ok := c.(*continuationCompleter[S]); ok {
		cc.mu.Lock()
		cc.late = late
		cc.mu.Unlock()
	}
}

// NewContinuation creates a yielded continuation handle and its paired completer.
// The ctx parameter is reserved for future deadline propagation into the continuation.
func NewContinuation[S any](ctx context.Context) (*Continuation[S], ContinuationCompleter[S]) {
	cont := &Continuation[S]{
		done:      make(chan struct{}),
		outcome:   make(chan continuationOutcome[S], 1),
		yieldedAt: time.Now(),
	}
	cont.setRequestContext(ctx)
	completer := &continuationCompleter[S]{cont: cont}
	cont.boundCompleter = completer
	return cont, completer
}
