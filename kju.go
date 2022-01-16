package kju

import (
	"context"
	"go.uber.org/zap"
	"time"
)

type stats struct {
	createdAt  time.Time
	queuedAt   time.Time
	startedAt  time.Time
	finishedAt time.Time
}

type Task struct {
	Handler string
	Data    map[string]string
	id      string
	status  taskStatus
	logger  *zap.Logger
	stats   *stats
}

type TaskHandler func(context.Context, *Task) error

type Options struct {
	// How often a worker should query for new tasks.
	// Default is 500ms.
	FetchInterval time.Duration
	// How many tasks can be executed at the same time.
	// Default is 50.
	ConcurrencyLimit int
	// How many tasks a worker should query and store for future execution.
	// Default is 2x ConcurrencyLimit.
	TaskQueueSize int
	// How many results can be stored in memory before other tasks are blocked.
	// Default is ConcurrencyLimit.
	ResultQueueSize int
	// How many results to send at once.
	// Default is 10.
	ResultBatchSize int
	// Timeout for task execution. Misbehaving tasks will not be killed,
	// the only thing a worker can do is to cancel a context.
	// Default is 1min.
	TaskTimeout time.Duration
	// Time after which graceful shutdown will be abandoned and the worker
	// will be killed.
	// Default is 2x TaskTimeout.
	GracefulShutdownTimeout time.Duration
}
