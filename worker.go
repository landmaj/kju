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
				setTaskStatuses(w.db, tasks)
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
							status:     statusLocked,
							timestamps: &timestamps{queuedAt: time.Now()},
						}
						err := rows.Scan(&task.id, &task.timestamps.createdAt, &task.Handler, &task.Data)
						if err != nil {
							w.logger.Error("failed to fetch a task", zap.Error(err))
						}
						task.logger = w.logger.With(
							zap.String("id", task.id),
							zap.String("handler", task.Handler),
						)
						tasks = append(tasks, task)
					}
					setTaskStatuses(tx, tasks)
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
				setTaskStatuses(w.db, tasks)
				tasks = nil
			}
			if !more {
				return
			}
		}
	}()
}

func (w *worker) runTask(task *Task, queue chan<- *Task) {
	w.wg.AddTask()
	go func() {
		defer w.wg.TaskDone()

		task.logger.Info("task running")
		handler := w.handlersMap[task.Handler]
		{
			var err error

			defer func() {
				r := recover()
				if r != nil {
					err = fmt.Errorf("%v", r)
				}
				logger := task.logger.With(
					zap.Duration(
						"time_lifecycle",
						task.timestamps.finishedAt.Sub(task.timestamps.createdAt).Round(time.Second),
					),
					zap.Duration(
						"time_queue",
						task.timestamps.startedAt.Sub(task.timestamps.queuedAt).Round(time.Millisecond)),
					zap.Duration(
						"time_execution",
						task.timestamps.finishedAt.Sub(task.timestamps.startedAt),
					),
				)
				if err != nil {
					task.status = statusFailed
					logger.Error("task failed")
				} else {
					task.status = statusFinished
					logger.Info("task finished")
				}
				queue <- task
			}()

			ctx, cancel := context.WithTimeout(context.Background(), w.cfg.TaskTimeout)
			defer cancel()

			task.timestamps.startedAt = time.Now()
			err = handler(ctx, task)
			task.timestamps.finishedAt = time.Now()
		}
	}()
}
