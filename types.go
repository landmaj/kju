package kju

import (
	"context"
	"time"
)

type Options struct {
	// How often a worker should query for new tasks.
	// Defaults to 1s.
	FetchInterval time.Duration
	// How many tasks can be executed at the same time.
	// Defaults to 50.
	ConcurrencyLimit uint
	// How many tasks a worker should query and store for future execution.
	// Defaults to 2x ConcurrencyLimit.
	QueueSize uint
	// Timeout for task execution. Misbehaving tasks will not be killed,
	// the only thing a worker can do is to cancel a context.
	// Defaults to no timeout.
	TaskTimeout time.Duration
	// Which tasks a worker should execute.
	// Defaults to all.
	Tasks []string
	// Time after which graceful shutdown will be abandoned and the worker
	// will be killed.
	// Defaults to TaskTimeout.
	GracefulShutdownTimeout time.Duration
	// If worker should exit when there are no more tasks to process.
	RunUntilCompletion bool
}

type Task struct {
	ID      string
	Name    string
	Created time.Time
	Data    map[string]string
}

type TaskHandler func(context.Context, *Task) (succeeded bool)

type Client interface {
	QueueTask(ctx context.Context, name string, data map[string]string) (id string, err error)
}

type Worker interface {
	Client
	RegisterTask(name string, handler TaskHandler) error
	Run() error
}

type taskStatus string

const (
	statusCreated    taskStatus = "created"
	statusQueued     taskStatus = "queued"
	statusInProgress taskStatus = "in_progress"
	statusSucceeded  taskStatus = "succeeded"
	statusFailed     taskStatus = "failed"
)
