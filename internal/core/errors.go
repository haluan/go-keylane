package core

import "errors"

var (
	ErrInvalidConfig      = errors.New("keylane: invalid config")
	ErrInvalidShardCount  = errors.New("keylane: invalid shard count")
	ErrInvalidWorkerCount = errors.New("keylane: invalid worker count")
	ErrInvalidQueueSize   = errors.New("keylane: invalid queue size per lane")
	ErrInvalidLane        = errors.New("keylane: invalid lane")
	ErrInvalidLaneQuota   = errors.New("keylane: invalid lane quota")
	ErrMissingLaneQuotas  = errors.New("keylane: missing lane quotas")
	ErrInvalidJob         = errors.New("keylane: invalid job")
	ErrInvalidKey         = errors.New("keylane: invalid key")
	ErrNilJobRun          = errors.New("keylane: nil job run function")
	ErrQueueFull          = errors.New("keylane: queue full")
	ErrQueueNotStarted    = errors.New("keylane: queue not started")
	ErrQueueAlreadyStarted = errors.New("keylane: queue already started")
)
