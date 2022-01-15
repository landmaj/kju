package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/landmaj/kju"

	"github.com/jackc/pgx/v4/pgxpool"
)

func main() {
	ctx := context.Background()
	db, err := pgxpool.Connect(
		ctx, "postgres://localhost:5432/fennel",
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	AddTasks(db, 1_000)

	cfg := &kju.Options{
		FetchInterval:           time.Millisecond * 200,
		ConcurrencyLimit:        50,
		QueueSize:               100,
		TaskTimeout:             time.Second * 10,
		Tasks:                   []string{},
		GracefulShutdownTimeout: time.Second * 60,
		RunUntilCompletion:      true,
	}
	worker := kju.NewWorker(db, cfg)
	worker.RegisterTask("benchmark", Handler)
	worker.Run()
}

func Handler(ctx context.Context, task *kju.Task) bool {
	return true
}

func AddTasks(db *pgxpool.Pool, count int) {
	client := kju.NewClient(db)

	for i := 0; i < count; i++ {
		client.QueueTask(context.TODO(), "benchmark", map[string]string{"ID": strconv.Itoa(i)})
	}
}
