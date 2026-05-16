package core

import "hash/fnv"

// HashKey returns a 64-bit FNV-1a hash of the given string key.
func HashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}
