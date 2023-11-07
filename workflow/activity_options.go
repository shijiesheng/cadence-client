// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package workflow

import (
	"time"

	"go.uber.org/cadence/internal"
)

// ActivityOptions stores all activity-specific invocation parameters that will be stored inside of a context.
type ActivityOptions = internal.ActivityOptions

// LocalActivityOptions doc
type LocalActivityOptions = internal.LocalActivityOptions

// RetryPolicy specify how to retry activity if error happens.
type RetryPolicy = internal.RetryPolicy

// WithActivityOptions makes a copy of the context and adds the
// passed in options to the context. If an activity options exists,
// it will be overwritten by the passed in value as a whole.
// So specify all the values in the options as necessary, as values
// in the existing context options will not be carried over.
func WithActivityOptions(ctx Context, options ActivityOptions) Context {
	return internal.WithActivityOptions(ctx, options)
}

// WithLocalActivityOptions makes a copy of the context and adds the
// passed in options to the context. If a local activity options exists,
// it will be overwritten by the passed in value.
func WithLocalActivityOptions(ctx Context, options LocalActivityOptions) Context {
	return internal.WithLocalActivityOptions(ctx, options)
}

// WithTaskList makes a copy of the current context and update the taskList
// field in its activity options. An empty activity options will be created
// if it does not exist in the original context.
func WithTaskList(ctx Context, name string) Context {
	return internal.WithTaskList(ctx, name)
}

// GetActivityTaskList returns tasklist where activity should be started
// if context is not an activity context then it will return nil
func GetActivityTaskList(ctx Context) *string {
	return internal.GetActivityTaskList(ctx)
}

// WithScheduleToCloseTimeout makes a copy of the current context and update
// the ScheduleToCloseTimeout field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
//
// Cadence time resolution is in seconds and the library uses math.Ceil(d.Seconds())
// to calculate the final value. This is subject to change in the future.
func WithScheduleToCloseTimeout(ctx Context, d time.Duration) Context {
	return internal.WithScheduleToCloseTimeout(ctx, d)
}

// WithScheduleToStartTimeout makes a copy of the current context and update
// the ScheduleToStartTimeout field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
//
// Cadence time resolution is in seconds and the library uses math.Ceil(d.Seconds())
// to calculate the final value. This is subject to change in the future.
func WithScheduleToStartTimeout(ctx Context, d time.Duration) Context {
	return internal.WithScheduleToStartTimeout(ctx, d)
}

// WithStartToCloseTimeout makes a copy of the current context and update
// the StartToCloseTimeout field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
//
// Cadence time resolution is in seconds and the library uses math.Ceil(d.Seconds())
// to calculate the final value. This is subject to change in the future.
func WithStartToCloseTimeout(ctx Context, d time.Duration) Context {
	return internal.WithStartToCloseTimeout(ctx, d)
}

// WithHeartbeatTimeout makes a copy of the current context and update
// the HeartbeatTimeout field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
//
// Cadence time resolution is in seconds and the library uses math.Ceil(d.Seconds())
// to calculate the final value. This is subject to change in the future.
func WithHeartbeatTimeout(ctx Context, d time.Duration) Context {
	return internal.WithHeartbeatTimeout(ctx, d)
}

// WithWaitForCancellation makes a copy of the current context and update
// the WaitForCancellation field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
func WithWaitForCancellation(ctx Context, wait bool) Context {
	return internal.WithWaitForCancellation(ctx, wait)
}

// WithRetryPolicy makes a copy of the current context and update
// the RetryPolicy field in its activity options. An empty activity
// options will be created if it does not exist in the original context.
func WithRetryPolicy(ctx Context, retryPolicy RetryPolicy) Context {
	return internal.WithRetryPolicy(ctx, retryPolicy)
}
