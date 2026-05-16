package core

import "testing"

func TestHashKeyDeterministic(t *testing.T) {
	key := "test-key-123"
	h1 := HashKey(key)
	h2 := HashKey(key)

	if h1 != h2 {
		t.Errorf("HashKey(%q) is non-deterministic: %d != %d", key, h1, h2)
	}

	// Spot check distinct inputs
	key2 := "test-key-456"
	h3 := HashKey(key2)
	if h1 == h3 {
		t.Errorf("HashKey(%q) and HashKey(%q) produced same hash: %d", key, key2, h1)
	}
}
