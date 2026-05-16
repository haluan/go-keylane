package core

import "testing"

func TestHashKeyDeterministic(t *testing.T) {
	key := "test-key-123"
	h1 := hashKey(key)
	h2 := hashKey(key)

	if h1 != h2 {
		t.Errorf("hashKey(%q) is non-deterministic: %d != %d", key, h1, h2)
	}

	// Spot check distinct inputs
	key2 := "test-key-456"
	h3 := hashKey(key2)
	if h1 == h3 {
		t.Errorf("hashKey(%q) and hashKey(%q) produced same hash: %d", key, key2, h1)
	}
}
