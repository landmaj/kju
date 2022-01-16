package kju

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

type Worker interface {
	Client
	RegisterTask(name string, handler TaskHandler) error
	Run() error
}

var _ Worker = &worker{}

type worker struct {
	Client
	db          *pgxpool.Pool
	handlersMap map[string]TaskHandler
	handlers    []string
	logger      *zap.Logger
	cfg         *Options
	wg          *customWaitGroup
}

func NewWorker(db *pgxpool.Pool, options *Options, logger *zap.Logger) Worker {
	if logger == nil {
		logger = newLogger("client", false)
	}
	return &worker{
		Client:      NewClient(db, logger),
		db:          db,
		handlersMap: make(map[string]TaskHandler),
		logger:      logger,
		cfg:         parseOptions(*options),
		wg:          newCustomWaitGroup(),
	}
}

func (w *worker) RegisterTask(name string, handler TaskHandler) error {
	if _, exists := w.handlersMap[name]; exists {
		w.logger.Error("duplicate handler", zap.String("handler", name))
		return errors.New("duplicate handler")
	}
	w.handlersMap[name] = handler
	w.logger.Debug("handler registered", zap.String("handler", name))
	return nil
}

func (w *worker) Run() error {
	w.handlers = make([]string, len(w.handlersMap))
	i := 0
	for k := range w.handlersMap {
		w.handlers[i] = k
		i++
	}
	w.logger.Info(
		"worker starting",
		zap.Int("pid", os.Getpid()),
		zap.Strings("handlers", w.handlers),
	)

	quit := terminationHandler(w.logger, w.cfg.GracefulShutdownTimeout, w.wg)

	fetchQuit := make(chan struct{})
	w.fetchTasks(fetchQuit)

	select {
	case <-quit:
		w.logger.Debug("terminating fetchTasks")
		close(fetchQuit)
		w.wg.Wait()
		w.logger.Info("cleanup completed, shutting down")
		return nil
	}
}

func (w *worker) fetchTasks(quit <-chan struct{}) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		dispatchQueue := make(chan *Task, w.cfg.TaskQueueSize)
		w.dispatchTasks(dispatchQueue)

		for {
			select {
			case <-quit:
				w.logger.Debug("terminating dispatchTasks")
				close(dispatchQueue)
				w.logger.Debug("draining task queue")
				var tasks []*Task
				for {
					task, more := <-dispatchQueue
					if !more {
						break
					}
					task.status = statusCreated
					tasks = append(tasks, task)
				}
				w.logger.Debug("returning tasks to database")
				w.setTaskStatuses(tasks)
				return
			case <-time.After(w.cfg.FetchInterval):
				var tasks []*Task
				err := w.db.BeginFunc(context.TODO(), func(tx pgx.Tx) error {
					limit := cap(dispatchQueue) - len(dispatchQueue)
					rows, err := tx.Query(context.TODO(), queryLockTasks, string(statusCreated), w.handlers, limit)
					defer rows.Close()
					if err != nil {
						return err
					}
					for rows.Next() {
						task := &Task{
							status:   statusLocked,
							queuedAt: time.Now(),
						}
						err := rows.Scan(&task.id, &task.createdAt, &task.Handler, &task.Data)
						if err != nil {
							w.logger.Error("failed to fetch a task", zap.Error(err))
						}
						tasks = append(tasks, task)
					}
					w.setTaskStatusesTx(tx, tasks)
					return nil
				})
				if err != nil {
					w.logger.Error("failed to fetch tasks", zap.Error(err))
					continue
				}
				w.logger.Debug("fetch successful", zap.Int("tasks", len(tasks)))
				for _, task := range tasks {
					dispatchQueue <- task
				}
			}
		}
	}()
}

func (w *worker) dispatchTasks(queue <-chan *Task) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		finishQueue := make(chan *Task, w.cfg.ResultQueueSize)
		w.finishTasks(finishQueue)

		for {
			task, more := <-queue
			if !more {
				if w.wg.TaskCount() != 0 {
					w.logger.Debug(
						"waiting for all tasks to finish",
						zap.Int("tasks_running", w.wg.TaskCount()),
					)
					w.wg.WaitTasks()
				}
				w.logger.Debug("terminating finishTasks")
				close(finishQueue)
				return
			}
			for w.cfg.ConcurrencyLimit-w.wg.TaskCount() == 0 {
				time.Sleep(time.Millisecond)
			}
			w.runTask(task, finishQueue)
		}
	}()
}

func (w *worker) finishTasks(queue <-chan *Task) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		var tasks []*Task
		for {
			task, more := <-queue
			if more {
				tasks = append(tasks, task)
			}
			if (len(tasks) == w.cfg.ResultBatchSize || !more) && len(tasks) > 0 {
				w.setTaskStatuses(tasks)
				tasks = nil
			}
			if !more {
				return
			}
		}
	}()
}

func (w *worker) setTaskStatuses(tasks []*Task) {
	batch := &pgx.Batch{}
	for _, t := range tasks {
		batch.Queue(querySetTaskStatus, string(t.status), t.id)
	}
	result := w.db.SendBatch(context.TODO(), batch)
	defer result.Close()
	for _, t := range tasks {
		_, err := result.Query()
		if err != nil {
			w.logger.Error(
				"failed to set task status",
				zap.String("ID", t.id),
				zap.String("status", string(t.status)),
			)
		}
	}
}

func (w *worker) setTaskStatusesTx(tx pgx.Tx, tasks []*Task) {
	batch := &pgx.Batch{}
	for _, t := range tasks {
		batch.Queue(querySetTaskStatus, string(t.status), t.id)
	}
	result := tx.SendBatch(context.TODO(), batch)
	defer result.Close()
	for _, t := range tasks {
		_, err := result.Query()
		if err != nil {
			w.logger.Error(
				"failed to set task status",
				zap.String("ID", t.id),
				zap.String("status", string(t.status)),
			)
		}
	}
}

func (w *worker) runTask(task *Task, queue chan<- *Task) {
	w.wg.AddTask()
	go func() {
		defer w.wg.TaskDone()

		logger := w.logger.With(
			zap.String("ID", task.id),
			zap.String("handler", task.Handler),
		)
		logger.Info("task running")
		handler := w.handlersMap[task.Handler]
		{
			defer func() {
				r := recover()
				if r != nil {
					task.status = statusFailed
					logger.Error("task panicked", zap.Error(fmt.Errorf("%v", r)))
				}
				queue <- task
			}()

			ctx, cancel := context.WithTimeout(context.Background(), w.cfg.TaskTimeout)
			defer cancel()
			start := time.Now()
			err := handler(ctx, task)
			now := time.Now()
			logger = logger.With(
				zap.Duration("execution", now.Sub(start)),
				zap.Duration("queued", now.Sub(task.queuedAt).Round(time.Millisecond)),
				zap.Duration("lifetime", now.Sub(task.createdAt).Round(time.Second)),
			)
			if err != nil {
				task.status = statusFailed
				logger.Error("task failed", zap.Error(err))
			} else {
				task.status = statusFinished
				logger.Info("task successful")
			}
		}

	}()
}
