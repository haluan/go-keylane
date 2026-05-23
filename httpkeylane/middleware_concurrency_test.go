// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package httpkeylane

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/haluan/go-keylane"
)

func TestHTTPRuntimeConcurrentRequestsComplete(t *testing.T) {
	q, _ := newTestQueue(t, withWorkerCount(4))
	var count atomic.Int32

	handler := Middleware(q, Config{
		KeyFunc:  HeaderKey("X-Tenant-ID"),
		LaneFunc: MethodLaneMapper(),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	const n = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	var fails []string
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			method := http.MethodGet
			if i%2 == 1 {
				method = http.MethodPost
			}
			req, err := http.NewRequest(method, srv.URL, nil)
			if err != nil {
				mu.Lock()
				fails = append(fails, fmt.Sprintf("request %d: NewRequest: %v", i, err))
				mu.Unlock()
				return
			}
			req.Header.Set("X-Tenant-ID", fmt.Sprintf("tenant-%d", i))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				mu.Lock()
				fails = append(fails, fmt.Sprintf("request %d: Do: %v", i, err))
				mu.Unlock()
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				mu.Lock()
				fails = append(fails, fmt.Sprintf("request %d: status %d", i, resp.StatusCode))
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	for _, msg := range fails {
		t.Error(msg)
	}
	if count.Load() != int32(n) {
		t.Errorf("handlers ran %d times, want %d", count.Load(), n)
	}
}

func TestHTTPRuntimeConcurrentSameKeyShardAffinity(t *testing.T) {
	obs := newTestRequestObserver()
	q, _ := newTestQueue(t, withShardCount(8), withWorkerCount(4), withRequestHooks(obs.hooks()))

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("affinity-key"),
		LaneFunc: StaticLane(keylane.Lane("default")),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	const n = 16
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			resp, err := http.Get(srv.URL)
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	started := obs.waitStarted(t, n)
	shard := started[0].ShardID
	for i, s := range started {
		if s.ShardID != shard {
			t.Errorf("request %d ShardID = %d, want %d", i, s.ShardID, shard)
		}
	}
}

func TestHTTPRuntimeConcurrentMixedLaneCounters(t *testing.T) {
	q, _ := newTestQueue(t, withWorkerCount(4))
	readBefore := laneCounter(t, q, LaneRead)
	writeBefore := laneCounter(t, q, LaneWrite)

	handler := Middleware(q, Config{
		KeyFunc:  StaticKey("k"),
		LaneFunc: MethodLaneMapper(),
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(srv.URL)
			if err == nil {
				resp.Body.Close()
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(srv.URL, "text/plain", nil)
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	readAfter := laneCounter(t, q, LaneRead)
	writeAfter := laneCounter(t, q, LaneWrite)
	if readAfter <= readBefore {
		t.Errorf("read Accepted = %d, want > %d", readAfter, readBefore)
	}
	if writeAfter <= writeBefore {
		t.Errorf("write Accepted = %d, want > %d", writeAfter, writeBefore)
	}
}

func TestHTTPRuntimeConcurrentOverloadRejectsSafely(t *testing.T) {
	q := admissionPressureQueueStarted(t)
	var handlerRan, rejectCount atomic.Int32

	handler := Middleware(q, Config{
		KeyFunc:  HeaderKey("X-Tenant-ID"),
		LaneFunc: StaticLane(keylane.Lane("default")),
		Admission: AdmissionConfig{
			Enabled:          true,
			RejectAboveRatio: 0.90,
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerRan.Add(1)
		w.WriteHeader(http.StatusOK)
	}))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	const n = 30
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
			req.Header.Set("X-Tenant-ID", "t")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			if resp.StatusCode == http.StatusServiceUnavailable {
				rejectCount.Add(1)
			}
			resp.Body.Close()
		}(i)
	}
	wg.Wait()

	if handlerRan.Load() == 0 && rejectCount.Load() == 0 {
		t.Error("expected some handler runs or HTTP 503 rejects")
	}
}
