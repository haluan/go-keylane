package core

// ReadyQueue is a channel of shard IDs that are ready to be processed.
// The capacity of the channel should be equal to the shard count.
type ReadyQueue chan int
