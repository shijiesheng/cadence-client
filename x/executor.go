package x

import (
	"go.uber.org/cadence/internal/executor"
	"go.uber.org/cadence/workflow"
)

// Executor is a interface that allows to add futures to an executor and execute them in batch
type Executor interface {
	AddFuture(factory func(ctx workflow.Context) workflow.Future, valuePtr interface{}) error
	Execute(ctx workflow.Context) error
}

// NewBatchExecutor creates a new batch executor
func NewBatchExecutor(ctx workflow.Context, batchSize int) Executor {
	return executor.NewBatchExecutor(ctx, batchSize)
}
