package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/landmaj/kju"

	"github.com/jackc/pgx/v4/pgxpool"
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

	cfg := &kju.Options{
		FetchInterval:           time.Millisecond * 100,
		ConcurrencyLimit:        100,
		QueueSize:               1000,
		TaskTimeout:             time.Second * 10,
		GracefulShutdownTimeout: time.Second * 60,
	}
	worker := kju.NewWorker(db, cfg)
	err = worker.RegisterTask("benchmark", Handler)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to register handler: %v\n", err)
		os.Exit(1)
	}
	_ = worker.Run()
}

func Handler(ctx context.Context, task *kju.Task) bool {
	return true
}
