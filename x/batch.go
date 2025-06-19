package x

import (
	"go.uber.org/cadence/internal/batch"
	"go.uber.org/cadence/workflow"
)

// Executor is a interface that allows to add futures to an executor and execute them in batch
type BatchFuture interface {
	workflow.Future
	GetFutures() []workflow.Future
}

// NewBatchExecutor creates a new batch executor
func NewBatchFuture(ctx workflow.Context, batchSize int, factories []func(ctx workflow.Context) workflow.Future) (BatchFuture, error) {
	return batch.NewBatchFuture(ctx, batchSize, factories)
}
