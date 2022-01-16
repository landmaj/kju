package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/landmaj/kju"

	"github.com/jackc/pgx/v4/pgxpool"
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

	cfg := &kju.Options{
		FetchInterval:           time.Millisecond * 100,
		ConcurrencyLimit:        50,
		TaskQueueSize:           100,
		ResultQueueSize:         50,
		ResultBatchSize:         10,
		TaskTimeout:             time.Second * 5,
		GracefulShutdownTimeout: time.Second * 10,
	}
	loggerCfg := zap.NewProductionConfig()
	loggerCfg.OutputPaths = []string{"stdout", "log.json"}
	logger, _ := loggerCfg.Build()

	worker := kju.NewWorker(db, cfg, logger)
	_ = worker.RegisterTask("benchmark", Handler)
	_ = worker.Run()
}

func Handler(ctx context.Context, task *kju.Task) error {
	return nil
}
