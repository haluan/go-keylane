package core

import "testing"

func TestReadyQueueBounded(t *testing.T) {
	size := 5
	rq := make(ReadyQueue, size)

	// Fill it
	for i := 0; i < size; i++ {
		rq <- i
	}

	// Try non-blocking send
	select {
	case rq <- 999:
		t.Error("ReadyQueue should be full and non-blocking send should fail")
	default:
		// success
	}
}
