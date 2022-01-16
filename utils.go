package kju

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

type taskStatus string

const (
	statusCreated  taskStatus = "created"
	statusLocked   taskStatus = "locked"
	statusFinished taskStatus = "finished"
	statusFailed   taskStatus = "failed"
)

const (
	queryInsertTask    = "INSERT INTO tasks (status, handler, data) VALUES ($1, $2, $3) RETURNING id"
	querySetTaskStatus = "UPDATE tasks SET status=$1 WHERE id=$2"
	queryLockTasks     = "SELECT id, created, handler, data FROM tasks WHERE status=$1 AND handler=any($2) ORDER BY created LIMIT $3 FOR UPDATE SKIP LOCKED"
)

func newLogger(name string, devMode bool) *zap.Logger {
	var logger *zap.Logger
	var err error
	if devMode {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		panic(err)
	}
	return logger.Named("kju").Named(name)
}

func parseOptions(options Options) *Options {
	if options.FetchInterval < 0 {
		panic("negative FetchInterval")
	} else if options.FetchInterval == 0 {
		options.FetchInterval = time.Millisecond * 500
	}

	if options.ConcurrencyLimit < 0 {
		panic("negative ConcurrencyLimit")
	} else if options.ConcurrencyLimit == 0 {
		options.ConcurrencyLimit = 50
	}

	if options.TaskQueueSize < 0 {
		panic("negative TaskQueueSize")
	} else if options.TaskQueueSize == 0 {
		options.TaskQueueSize = 2 * options.ConcurrencyLimit
	}

	if options.ResultQueueSize < 0 {
		panic("negative ResultQueueSize")
	} else if options.ResultQueueSize == 0 {
		options.ResultQueueSize = options.ConcurrencyLimit
	}

	if options.ResultBatchSize < 0 {
		panic("negative ResultBatchSize")
	} else if options.ResultBatchSize == 0 {
		options.ResultBatchSize = 10
	}

	if options.TaskTimeout < 0 {
		panic("negative TaskTimeout")
	} else if options.TaskTimeout == 0 {
		options.TaskTimeout = time.Minute
	}

	if options.GracefulShutdownTimeout < 0 {
		panic("negative GracefulShutdownTimeout")
	} else if options.GracefulShutdownTimeout == 0 {
		options.GracefulShutdownTimeout = 2 * options.TaskTimeout
	}

	return &options
}

func terminationHandler(
	logger *zap.Logger,
	timeout time.Duration,
	wg *customWaitGroup,
) <-chan struct{} {
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	quit := make(chan struct{})
	go func() {
		defer close(quit)
		s := <-signalChan
		logger.Info(
			"graceful shutdown initiated",
			zap.String("source", s.String()),
		)
		go func() {
			<-time.After(timeout)
			logger.Fatal(
				"timeout exceeded, forcefully shutting down",
				zap.Duration("timeout", timeout),
				zap.Int("tasks_running", wg.TaskCount()),
			)
		}()
	}()
	return quit
}
