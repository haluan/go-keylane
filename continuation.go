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
//
// Experimental: may change before v1.0.
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

	// lateOnce ensures at most one late-completion hook per continuation (resolver vs completer races).
	lateOnce sync.Once
	// registryLateOnce ensures the registry late counter increments at most once per continuation.
	registryLateOnce sync.Once

	boundCompleter ContinuationCompleter[S]
}

func (cont *Continuation[S]) recordLateRegistry(q *Queue) {
	cont.registryLateOnce.Do(func() {
		q.continuationReg.recordLate()
	})
}

// recordLateCompletion increments late-completion diagnostics at most once for this continuation.
func (cont *Continuation[S]) recordLateCompletion(q *Queue, exec StageExecutionContext, kind ContinuationOutcomeKind, err error) {
	if kind == continuationOutcomeCancel {
		return
	}
	cont.recordLateRegistry(q)
	cont.lateOnce.Do(func() {
		if err == nil {
			err = ErrContinuationLate
		}
		q.emitContinuationLate(cont.lateObservation(exec, kind, err))
	})
}

// recordLateCompleterIgnored counts a completer call after the continuation is already terminal.
func (cont *Continuation[S]) recordLateCompleterIgnored(q *Queue, exec StageExecutionContext) {
	cont.recordLateRegistry(q)
	cont.lateOnce.Do(func() {
		q.emitContinuationLate(cont.lateObservation(exec, continuationOutcomeComplete, ErrContinuationLate))
	})
}

func (cont *Continuation[S]) lateObservation(exec StageExecutionContext, kind ContinuationOutcomeKind, err error) ContinuationObservation {
	if err == nil {
		err = ErrContinuationLate
	}
	return continuationObsFromExec(
		cont.ID, exec, time.Since(cont.yieldedAt), 0,
		classifyOutcomeRequestOutcome(kind),
		classifyOutcomeFailureKind(kind),
		err,
	)
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
// ContinuationCompleter drives resolution of a yielded continuation (Complete, Fail, or Cancel).
// Methods are exactly-once: the first call wins. Experimental: may change before v1.0.
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
	once  sync.Once
	mu    sync.Mutex
	cont  *Continuation[S]
	late  func()
	queue *Queue
	exec  StageExecutionContext
}

func (c *continuationCompleter[S]) invokeLate() {
	c.mu.Lock()
	q, exec, late := c.queue, c.exec, c.late
	c.mu.Unlock()
	if q != nil && c.cont != nil {
		c.cont.recordLateCompleterIgnored(q, exec)
	}
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

// setContinuationLateHandler registers late-completion accounting for a completer.
// When q is non-nil, ignored Complete/Fail calls increment registry diagnostics via recordLateCompleterIgnored.
// An optional late hook runs after registry accounting (used by tests with a standalone registry).
func setContinuationLateHandler[S any](c ContinuationCompleter[S], q *Queue, exec StageExecutionContext, late func()) {
	if cc, ok := c.(*continuationCompleter[S]); ok {
		cc.mu.Lock()
		cc.queue = q
		cc.exec = exec
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
