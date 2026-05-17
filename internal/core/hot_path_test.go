package core

import (
	"context"
	"testing"
)

func TestLaneRegistryResolvesLaneID(t *testing.T) {
	reg, err := NewLaneRegistry(map[string]int{
		"laneA": 1,
		"laneB": 2,
	})
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}

	idA, okA := reg.Lookup("laneA")
	idB, okB := reg.Lookup("laneB")

	if !okA || !okB {
		t.Error("expected to resolve both registered lanes")
	}
	if idA == idB {
		t.Errorf("expected different IDs, both got %v", idA)
	}
}

func TestLaneRegistryRejectsUnknownLane(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"laneA": 1})
	_, ok := reg.Lookup("unknown")
	if ok {
		t.Error("expected lookup for unknown lane to fail")
	}
}

func TestProcessShardUsesRegisteredLaneOrder(t *testing.T) {
	// LaneRegistry assigns IDs based on alphabetical sort order
	reg, _ := NewLaneRegistry(map[string]int{
		"laneB": 2,
		"laneA": 1,
	})

	idA, _ := reg.Lookup("laneA")
	idB, _ := reg.Lookup("laneB")

	if idA != 0 {
		t.Errorf("expected laneA to have ID 0, got %d", idA)
	}
	if idB != 1 {
		t.Errorf("expected laneB to have ID 1, got %d", idB)
	}
}

func TestProcessShardQuotaByLaneID(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{
		"laneA": 10,
		"laneB": 5,
	})

	s, _ := NewScheduler(1, 1, 100, reg)

	if s.laneQuotas[0] != 10 {
		t.Errorf("expected quota for lane 0 to be 10, got %d", s.laneQuotas[0])
	}
	if s.laneQuotas[1] != 5 {
		t.Errorf("expected quota for lane 1 to be 5, got %d", s.laneQuotas[1])
	}
}

func TestHotPathLaneQueueCapacityStable(t *testing.T) {
	q := newLaneQueue(5)
	initialCap := cap(q.items)

	for i := 0; i < 100; i++ {
		_ = q.push(InternalJob{})
		_, _ = q.pop()
	}

	if cap(q.items) != initialCap {
		t.Errorf("expected queue capacity to remain stable at %d, got %d", initialCap, cap(q.items))
	}
}

func TestHotPathProcessShardUsesLaneID(t *testing.T) {
	reg, _ := NewLaneRegistry(map[string]int{"default": 1})
	s, _ := NewScheduler(1, 1, 10, reg)
	job := InternalJob{KeyHash: 1, LaneID: 0, Run: dummyRun}

	_ = s.shards[0].Lanes[0].push(job)
	s.shards[0].Ready = true

	// Directly running processShard executes the job via LaneID slice lookup
	s.processShard(context.Background(), 0)

	if s.shards[0].Lanes[0].depth() != 0 {
		t.Error("expected job to be popped and processed")
	}
}

func TestHotPathNoUnboundedQueueGrowth(t *testing.T) {
	q := newLaneQueue(2)
	_ = q.push(InternalJob{})
	_ = q.push(InternalJob{})

	err := q.push(InternalJob{})
	if err == nil {
		t.Error("expected error pushing beyond queue capacity")
	}
}
