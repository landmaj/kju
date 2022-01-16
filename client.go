package kju

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

type Client interface {
	QueueTask(ctx context.Context, task *Task) (id string, err error)
	QueueTasks(ctx context.Context, tasks []*Task) ([]string, error)
}

var _ Client = &client{}

func NewClient(db *pgxpool.Pool, logger *zap.Logger) Client {
	if logger == nil {
		logger = newLogger("client", false)
	}
	return &client{db: db, logger: logger}
}

type client struct {
	db     *pgxpool.Pool
	logger *zap.Logger
}

func (c *client) QueueTask(ctx context.Context, task *Task) (id string, err error) {
	err = c.db.QueryRow(ctx, queryInsertTask, statusCreated, task.Handler, task.Data).Scan(&id)
	if err != nil {
		c.logger.Error(
			"failed to add a task",
			zap.String("task", task.Handler),
			zap.Error(err),
		)
		return "", fmt.Errorf("insert error: %w", err)
	}
	c.logger.Info(
		"new task created",
		zap.String("task", task.Handler),
		zap.String("ID", id),
	)
	return
}

func (c *client) QueueTasks(
	ctx context.Context, tasks []*Task) (taskIDs []string, err error) {
	batch := pgx.Batch{}
	for _, task := range tasks {
		batch.Queue(queryInsertTask, statusCreated, task.Handler, task.Data)
	}
	batchResult := c.db.SendBatch(ctx, &batch)
	defer batchResult.Close()
	for _, task := range tasks {
		var id string
		err = batchResult.QueryRow().Scan(&id)
		if err != nil {
			c.logger.Error(
				"failed to add a task",
				zap.String("task", task.Handler),
				zap.Error(err),
			)
			continue
		}
		taskIDs = append(taskIDs, id)
		c.logger.Info(
			"new task created",
			zap.String("task", task.Handler),
			zap.String("ID", id),
		)
	}
	return taskIDs, err
}
