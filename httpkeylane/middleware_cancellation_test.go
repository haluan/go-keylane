// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haluan/go-keylane"
)

func TestHTTPRuntimeCancelledBeforeExecutionSkipsHandler(t *testing.T) {
	hold := make(chan struct{})
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withWorkerCount(1), withRequestHooks(obs.hooks()))
	blockWorker(t, q, hold)

	var ran atomic.Bool
	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(ctx)

	done := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	obs.waitQueued(t, 1)
	cancel()
	close(hold)
	waitDone(t, done, "middleware did not return")

	if ran.Load() {
		t.Error("handler ran after cancel while queued")
	}
}

func TestHTTPRuntimeDeadlineWhileQueuedSkipsHandler(t *testing.T) {
	hold := make(chan struct{})
	q, _ := newTestQueue(t, withWorkerCount(1))
	blockWorker(t, q, hold)

	var ran atomic.Bool
	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran.Store(true)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	close(hold)

	if ran.Load() {
		t.Error("handler ran after deadline while queued")
	}
}

func TestHTTPRuntimeCancellationWhileRunningReachesHandler(t *testing.T) {
	q, _ := newTestQueue(t)
	started := make(chan struct{})
	errDone := make(chan error, 1)

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		errDone <- r.Context().Err()
	}))

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(ctx)

	mwDone := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		close(mwDone)
	}()

	<-started
	cancel()

	var gotErr error
	select {
	case gotErr = <-errDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for handler context error")
	}
	waitDone(t, mwDone, "middleware did not return")

	if !errors.Is(gotErr, context.Canceled) {
		t.Errorf("handler ctx err = %v, want Canceled", gotErr)
	}
}

func TestHTTPRuntimeHandlerIgnoringCancellationCompletesSafely(t *testing.T) {
	q, _ := newTestQueue(t)
	handlerStarted := make(chan struct{})
	handlerDone := make(chan struct{})

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerStarted)
		<-handlerDone
		w.WriteHeader(http.StatusOK)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(ctx)

	mwDone := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		close(mwDone)
	}()

	<-handlerStarted
	cancel()
	close(handlerDone)
	waitDone(t, mwDone, "middleware blocked after cancel")
}

func TestHTTPRuntimeAwaitTimeoutDoesNotLeak(t *testing.T) {
	// HTTP middleware uses r.Context() for both SubmitRequest execution and future.Await.
	// Client disconnect/cancel must not leave blocked goroutines.
	before := runtime.NumGoroutine()
	hold := make(chan struct{})
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withWorkerCount(1), withRequestHooks(obs.hooks()))
	blockWorker(t, q, hold)

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(ctx)

	done := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	obs.waitQueued(t, 1)
	cancel()
	close(hold)
	waitDone(t, done, "cancelled queued request did not finish")

	eventuallyNoGoroutineGrowth(t, before, 4)
}
