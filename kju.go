package kju

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	_ Client = &kju{}
	_ Worker = &kju{}
)

type kju struct {
	db       *pgxpool.Pool
	handlers map[string]TaskHandler
	log      *log.Logger
	errorLog *log.Logger
	cfg      *Options
}

func NewClient(db *pgxpool.Pool) Client {
	return &kju{
		db:       db,
		log:      log.New(os.Stdout, "[kju.client] ", log.Ldate|log.Ltime|log.LUTC),
		errorLog: log.New(os.Stderr, "[kju.client] ", log.Ldate|log.Ltime|log.LUTC),
	}
}

func NewWorker(db *pgxpool.Pool, options *Options) Worker {
	return &kju{
		db:       db,
		handlers: make(map[string]TaskHandler),
		log:      log.New(os.Stdout, "[kju.worker] ", log.Ldate|log.Ltime|log.LUTC),
		errorLog: log.New(os.Stderr, "[kju.worker] ", log.Ldate|log.Ltime|log.LUTC),
		cfg:      parseOptions(*options),
	}
}

func parseOptions(options Options) *Options {
	if options.FetchInterval == 0 {
		options.FetchInterval = time.Second
	}
	if options.ConcurrencyLimit == 0 {
		options.ConcurrencyLimit = 50
	}
	if options.QueueSize == 0 {
		options.QueueSize = options.ConcurrencyLimit * 2
	}
	if options.GracefulShutdownTimeout == 0 {
		options.GracefulShutdownTimeout = options.TaskTimeout
	}
	return &options
}

func (f *kju) QueueTask(
	ctx context.Context, name string, data map[string]string,
) (id string, err error) {
	query := "INSERT INTO tasks (status, task, Data) VALUES ($1, $2, $3) RETURNING ID"
	err = f.db.QueryRow(ctx, query, statusCreated, name, data).Scan(&id)
	if err != nil {
		f.errorLog.Println("failed to add a task:", err)
		return "", fmt.Errorf("postgresql error: %w", err)
	}
	f.log.Println("new task created:", name, id)
	return
}

func (f *kju) QueueTasks(
	ctx context.Context, tasks []struct {
		Name string
		Data map[string]string
	}) (taskIDs []string, err error) {
	batch := pgx.Batch{}
	for _, task := range tasks {
		batch.Queue(
			"INSERT INTO tasks (status, task, Data) VALUES ($1, $2, $3) RETURNING ID",
			statusCreated, task.Name, task.Data,
		)
	}
	batchResult := f.db.SendBatch(ctx, &batch)
	defer batchResult.Close()
	for i := 0; i < len(tasks); i++ {
		var id string
		err = batchResult.QueryRow().Scan(&id)
		if err != nil {
			f.errorLog.Println("failed to add a task:", err)
			continue
		}
		taskIDs = append(taskIDs, id)
	}
	f.log.Printf("%d new tasks created\n", len(taskIDs))
	return taskIDs, err
}

func (f *kju) RegisterTask(name string, handler TaskHandler) error {
	if _, exists := f.handlers[name]; exists {
		return fmt.Errorf("duplicate handler: %s", name)
	}
	f.handlers[name] = handler
	f.log.Println("handler registered:", name)
	return nil
}

func (f *kju) Run() error {
	f.log.Println("starting worker")
	ctx := f.setupTerminationHandler()
	wg := NewWaitGroup()
	queue := make(chan *Task, f.cfg.QueueSize)

	wg.Add(2)
	go f.fetchTasks(ctx, wg, queue)
	go f.dispatchTasks(ctx, wg, queue)

	start := time.Now()
	select {
	case <-ctx.Done():
		wg.Add(1)
		go f.clearQueue(wg, queue)
		if f.cfg.GracefulShutdownTimeout == 0 {
			<-wg.Wait()
		} else {
			running := wg.WaitTimeout(f.cfg.GracefulShutdownTimeout)
			if running != 0 {
				f.errorLog.Println("%d tasks still running, forceful shutdown")
				return errors.New("timed out shutdown")
			}
		}
		f.log.Println("shutting down")
		return nil
	case <-wg.Wait():
		f.log.Printf("finished after %s\n", time.Now().Sub(start).Round(time.Millisecond))
		f.log.Println("shutting down")
		return nil
	}
}

func (f *kju) setupTerminationHandler() context.Context {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		<-c
		fmt.Println()
		f.log.Println("graceful shutdown initiated")
	}()
	return ctx
}

func (f *kju) fetchTasks(
	ctx context.Context,
	wg WaitGroup,
	queue chan<- *Task,
) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			close(queue)
			return
		case <-time.After(f.cfg.FetchInterval):
			ctx := context.TODO()
			var tasks []*Task
			err := f.db.BeginFunc(ctx, func(tx pgx.Tx) error {
				limit := cap(queue) - len(queue)
				rows, err := tx.Query(
					ctx,
					"SELECT id, created, task, data FROM tasks WHERE status=$1 ORDER BY created LIMIT $2 FOR UPDATE",
					string(statusCreated),
					limit,
				)
				defer rows.Close()
				if err != nil {
					f.errorLog.Println("error fetching tasks:", err)
					return err
				}
				var counter int
				var ids []string
				for rows.Next() {
					task := Task{}
					err := rows.Scan(&task.ID, &task.Created, &task.Name, &task.Data)
					if err != nil {
						f.errorLog.Println("error retrieving task:", err)
						continue
					}
					tasks = append(tasks, &task)
					ids = append(ids, task.ID)
					counter++
				}
				_, err = tx.Exec(ctx, "UPDATE tasks SET status=$1 WHERE id=any($2)", statusQueued, ids)
				if err != nil {
					f.errorLog.Println("error updating task statuses:", err)
					return err
				}
				if counter != 0 {
					f.log.Printf("%d task(s) found", counter)
				}
				return nil
			})
			if err != nil {
				f.errorLog.Println("error fetching tasks:", err)
			}
			for _, task := range tasks {
				queue <- task
			}
		}
	}
}

func (f *kju) dispatchTasks(
	ctx context.Context,
	wg WaitGroup,
	queue <-chan *Task,
) {
	defer wg.Done()
	time.Sleep(time.Millisecond * 100) // wait for the app to start
	for {
		err := ctx.Err()
		if errors.Is(err, context.Canceled) {
			return
		}
		if wg.TaskCount() < int(f.cfg.ConcurrencyLimit) {
			select {
			case task, chOpen := <-queue:
				if !chOpen {
					return
				}
				wg.AddTask()
				go f.taskRunner(wg, task)
			case <-time.After(time.Millisecond):
				break
			}
		}
	}
}

func (f *kju) taskRunner(wg WaitGroup, task *Task) {
	defer wg.TaskDone()
	ctx := context.TODO()
	start := time.Now()
	f.log.Printf("[%s] starting task\n", task.ID)
	_, err := f.db.Exec(ctx, "UPDATE tasks SET status=$1 WHERE id=$2", string(statusInProgress), task.ID)
	if err != nil {
		f.errorLog.Printf("[%s] error updating status: %s\n", task.ID, err)
		return
	}
	handler, exists := f.handlers[task.Name]
	if !exists {
		f.errorLog.Println("handler does not exist:", task.Name)
		_, err := f.db.Exec(ctx, "UPDATE tasks SET status=$1 WHERE id=$2", string(statusFailed), task.ID)
		if err != nil {
			f.errorLog.Printf("[%s] error updating status: %s\n", task.ID, err)
		}
		return
	}
	var succeeded bool
	var handlerTime time.Duration
	{
		defer f.catchPanic(ctx, task.ID)
		if f.cfg.TaskTimeout != 0 {
			ctx, cancel := context.WithTimeout(ctx, f.cfg.TaskTimeout)
			defer cancel()
			start := time.Now()
			succeeded = handler(ctx, task)
			handlerTime = time.Now().Sub(start)
		}
	}
	var status taskStatus
	if succeeded {
		status = statusSucceeded
	} else {
		status = statusFailed
	}
	_, err = f.db.Exec(ctx, "UPDATE tasks SET status=$1 WHERE id=$2", string(status), task.ID)
	if err != nil {
		f.errorLog.Printf("[%s] error updating status: %s\n", task.ID, err)
		return
	}
	f.log.Printf(
		"[%s] %s after %s; time spent in handler: %s",
		task.ID,
		status,
		time.Now().Sub(start).Round(time.Millisecond),
		handlerTime.Round(time.Millisecond),
	)
}

func (f *kju) catchPanic(ctx context.Context, id string) {
	if r := recover(); r != nil {
		f.errorLog.Printf("[%s] panic recovered: %s\n", id, r)
		_, err := f.db.Exec(ctx, "UPDATE tasks SET status=$1 WHERE id=$2", string(statusFailed), id)
		if err != nil {
			f.errorLog.Printf("[%s] error updating status: %s\n", id, err)
			return
		}
	}
}

func (f *kju) clearQueue(
	wg WaitGroup,
	queue <-chan *Task,
) {
	defer wg.Done()
	var ids []string
	for task := range queue {
		ids = append(ids, task.ID)
	}
	if len(ids) == 0 {
		return
	}
	f.log.Printf("freeing %d queued tasks", len(ids))
	_, err := f.db.Exec(context.TODO(), "UPDATE tasks SET status=$1 WHERE id=any($2)", statusCreated, ids)
	if err != nil {
		f.errorLog.Println("error updating task statuses:", err)
		return
	}
}
