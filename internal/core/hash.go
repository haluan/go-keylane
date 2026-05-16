package core

import "hash/fnv"

// hashKey returns a 64-bit FNV-1a hash of the given string key.
func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}
