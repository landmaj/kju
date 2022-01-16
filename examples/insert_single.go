package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/landmaj/kju"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()
	db, err := pgxpool.Connect(
		ctx, "postgres://localhost:5432/kju",
	)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	AddTasks(db, 10_000)
}

func AddTasks(db *pgxpool.Pool, count int) {
	logger, _ := zap.NewDevelopment()
	client := kju.NewClient(db, logger)

	start := time.Now()
	for i := 1; i != count; i++ {
		_, _ = client.QueueTask(
			context.TODO(),
			&kju.Task{
				Handler: "benchmark", Data: map[string]string{"ID": strconv.Itoa(i)},
			},
		)
	}
	logger.Info(
		"finished",
		zap.Duration("runtime", time.Now().Sub(start).Round(time.Millisecond)),
		zap.Int("rows", count),
	)
}
