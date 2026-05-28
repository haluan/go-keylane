// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

package keylane

import (
	"context"
	"time"
)

// StageExecutionContext is immutable request/stage execution metadata stored in context.Context.
// It is not a replacement for context.Context; use StageExecutionFromContext to read it.
//
// Experimental: may change before v1.0. RequestID and Key are for correlated logs, not metric labels.
type StageExecutionContext struct {
	RequestID string
	Key       string
	Lane      Lane
	ShardID   int

	Transport string
	Operation string

	Stage      StageMeta
	StageIndex int
	StageCount int

	Attempt int

	QueueWait time.Duration
	Deadline  DeadlineBudgetSnapshot
}

type stageExecutionKey struct{}

// ContextWithStageExecution returns ctx with a snapshot of execution metadata.
func ContextWithStageExecution(ctx context.Context, exec StageExecutionContext) context.Context {
	return context.WithValue(ctx, stageExecutionKey{}, exec)
}

// StageExecutionFromContext returns execution metadata attached to ctx.
func StageExecutionFromContext(ctx context.Context) (StageExecutionContext, bool) {
	exec, ok := ctx.Value(stageExecutionKey{}).(StageExecutionContext)
	return exec, ok
}

// RequestMetaFromExecution builds RequestMeta from execution metadata.
func RequestMetaFromExecution(exec StageExecutionContext) RequestMeta {
	return RequestMeta{
		RequestID: exec.RequestID,
		Key:       exec.Key,
		Lane:      exec.Lane,
		Transport: exec.Transport,
		Operation: exec.Operation,
	}
}

// StageMetaFromContext returns the active stage metadata when present.
func StageMetaFromContext(ctx context.Context) (StageMeta, bool) {
	exec, ok := StageExecutionFromContext(ctx)
	if !ok {
		return StageMeta{}, false
	}
	return exec.Stage, true
}

func baseExecutionContext(
	meta RequestMeta,
	shardID int,
	queueWait time.Duration,
	attempt int,
	stage StageMeta,
	stageIndex, stageCount int,
	deadline DeadlineBudgetSnapshot,
) StageExecutionContext {
	return StageExecutionContext{
		RequestID:  meta.RequestID,
		Key:        meta.Key,
		Lane:       meta.Lane,
		ShardID:    shardID,
		Transport:  meta.Transport,
		Operation:  meta.Operation,
		Stage:      stage,
		StageIndex: stageIndex,
		StageCount: stageCount,
		Attempt:    attempt,
		QueueWait:  queueWait,
		Deadline:   deadline,
	}
}

func withPipelineStage(exec StageExecutionContext, stage StageMeta, stageIndex, stageCount int, runtime time.Duration, deadline DeadlineBudgetSnapshot) StageExecutionContext {
	exec.Stage = stage
	exec.StageIndex = stageIndex
	exec.StageCount = stageCount
	exec.Deadline = deadline
	exec.Deadline.Runtime = runtime
	return exec
}

func singleRequestExecutionContext(meta RequestMeta, shardID int, queueWait time.Duration, attempt int, deadline DeadlineBudgetSnapshot) StageExecutionContext {
	return baseExecutionContext(
		meta, shardID, queueWait, attempt,
		StageMeta{Name: StageBusiness}, 0, 1,
		deadline,
	)
}

func attemptFromContext(ctx context.Context) int {
	if exec, ok := StageExecutionFromContext(ctx); ok && exec.Attempt > 0 {
		return exec.Attempt
	}
	return 1
}
