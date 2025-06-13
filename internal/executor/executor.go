package executor

import (
	"errors"

	"go.uber.org/multierr"

	"go.uber.org/cadence/internal"
)

// type Executor interface {
// 	AddFuture(factory func(ctx internal.Context) internal.Future, valuePtr interface{}) error
// 	Execute(ctx internal.Context) error
// }

// type batchExecutor struct {
// 	isRunning bool
// 	factories []func(ctx internal.Context) internal.Future
// 	valuePtrs []interface{}

// 	batchSize int
// }

// type futureWithResult struct {
// 	future   internal.Future
// 	valuePtr interface{}
// }

// func (e *batchExecutor) AddFuture(factory func(ctx internal.Context) internal.Future, valuePtr interface{}) error {
// 	if e.isRunning {
// 		return errors.New("executor is already running")
// 	}
// 	e.factories = append(e.factories, factory)
// 	e.valuePtrs = append(e.valuePtrs, valuePtr)
// 	return nil
// }

// func (e *batchExecutor) Execute(ctx internal.Context) error {
// 	if e.isRunning {
// 		return errors.New("executor is already running")
// 	}
// 	e.isRunning = true

// 	futuresToProcess := internal.NewNamedChannel(ctx, "batch-executor-features-to-process")
// 	wg := internal.NewWaitGroup(ctx)

// 	buffered := internal.NewBufferedChannel(ctx, e.batchSize)
// 	var errs error
// 	wg.Add(1)

// 	// processor of features asynchronously
// 	internal.GoNamed(ctx, "batch-executor-processor-loop", func(ctx internal.Context) {
// 		defer wg.Done()
// 		for {
// 			var futureWithResult futureWithResult
// 			ok := futuresToProcess.Receive(ctx, &futureWithResult)
// 			if !ok {
// 				break
// 			}
// 			wg.Add(1)
// 			internal.GoNamed(ctx, "batch-executor-feature-processor", func(ctx internal.Context) {
// 				defer wg.Done()
// 				err := futureWithResult.future.Get(ctx, futureWithResult.valuePtr)
// 				errs = multierr.Append(errs, err)
// 				buffered.Receive(ctx, nil)
// 			})
// 		}
// 	})

// 	// submit all futures within concurrency limit, wait to schedule until it's ready
// 	for i := range e.factories {
// 		buffered.Send(ctx, nil)
// 		futuresToProcess.Send(ctx, futureWithResult{
// 			future:   e.factories[i](ctx),
// 			valuePtr: e.valuePtrs[i],
// 		})
// 	}

// 	// close the channel to signal the task result collector that no more tasks are coming
// 	futuresToProcess.Close()

// 	wg.Wait(ctx)

// 	return errs
// }

// func NewBatchExecutor(ctx internal.Context, batchSize int) Executor {
// 	return &batchExecutor{
// 		batchSize: batchSize,
// 	}
// }

type futureWithIndex struct {
	future internal.Future
	index int
}

type BatchFuture interface {
	GetFutures() []internal.Future
	Get(ctx internal.Context, valuePtr... interface{}) error
}

type batchFutureImpl struct {
	futures []internal.Future
	settables []internal.Settable
	factories []func(ctx internal.Context) internal.Future
}

func (b *batchFutureImpl) GetFutures() []internal.Future {
	return b.futures
}


func NewBatchFuture(ctx internal.Context, batchSize int, factories []func(ctx internal.Context) internal.Future) (BatchFuture, error) {

}


func ()(ctx internal.Context, batchSize int, factories []func(ctx internal.Context) internal.Future) (BatchFuture, error) {

	futuresToReturn := make([]internal.Future, len(factories))
	settablesToReturn := make([]internal.Settable, len(factories))

	futuresToProcess := internal.NewNamedChannel(ctx, "batch-executor-features-to-process")
	wg := internal.NewWaitGroup(ctx)

	buffered := internal.NewBufferedChannel(ctx, batchSize)
	var errs error
	wg.Add(1)

	// processor of features asynchronously
	internal.GoNamed(ctx, "batch-executor-processor-loop", func(ctx internal.Context) {
		defer wg.Done()
		for {
			var f futureWithIndex
			ok := futuresToProcess.Receive(ctx, &f)
			if !ok {
				break
			}
			wg.Add(1)
			internal.GoNamed(ctx, "batch-executor-feature-processor", func(ctx internal.Context) {
				defer wg.Done()

				// fork a future and chain it to the processed future for user to get the result
				future, settable :=internal.NewFuture(ctx)
				settable.Chain(f.future)

				// complete the future to process and chain it to the future to return
				// this way user can decide when to get the result
				err := f.future.Get(ctx, nil)
				errs = multierr.Append(errs, err)
				futuresToReturn[f.index] = future
				settablesToReturn[f.index] = settable
				buffered.Receive(ctx, nil)
			})
		}
	})

	// submit all futures within concurrency limit, wait to schedule until it's ready
	for i := range factories {
		buffered.Send(ctx, nil)
		futuresToProcess.Send(ctx, futureWithIndex{
			future:   factories[i](ctx),
			index:    i,
		})
	}

	// close the channel to signal the task result collector that no more tasks are coming
	futuresToProcess.Close()

	wg.Wait(ctx)

	return errs
}
