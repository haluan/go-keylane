package keylane

import (
	"errors"

	"github.com/haluan/go-keylane/internal/core"
)

var (
	ErrInvalidConfig      = core.ErrInvalidConfig
	ErrInvalidShardCount  = core.ErrInvalidShardCount
	ErrInvalidWorkerCount = core.ErrInvalidWorkerCount
	ErrInvalidQueueSize   = core.ErrInvalidQueueSize
	ErrInvalidLane        = core.ErrInvalidLane
	ErrInvalidLaneQuota   = core.ErrInvalidLaneQuota
	ErrMissingLaneQuotas  = core.ErrMissingLaneQuotas
	ErrNilQueue           = errors.New("keylane: nil queue")
	ErrInvalidJob         = core.ErrInvalidJob
	ErrInvalidKey         = core.ErrInvalidKey
	ErrNilJobRun          = core.ErrNilJobRun
	ErrQueueFull          = core.ErrQueueFull
	ErrQueueNotStarted    = core.ErrQueueNotStarted
	ErrQueueAlreadyStarted = core.ErrQueueAlreadyStarted
)
