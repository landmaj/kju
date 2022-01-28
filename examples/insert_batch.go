package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
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

	//AddTasksBatch(db, 10_000)
	AddDomains(db, 1000)
}

func AddTasksBatch(db *pgxpool.Pool, count int) {
	logger, _ := zap.NewDevelopment()
	client := kju.NewClient(db, logger)

	start := time.Now()
	var queue []*kju.Task
	for i := 0; i != count; i++ {
		queue = append(queue, &kju.Task{
			Handler: "benchmark", Data: map[string]string{"ID": strconv.Itoa(i)},
		})
		if i%100 == 0 || i == count-1 {
			_, _ = client.QueueTasks(context.TODO(), queue)
			queue = nil
		}
	}
	logger.Info(
		"finished",
		zap.Duration("runtime", time.Now().Sub(start).Round(time.Millisecond)),
		zap.Int("rows", count),
	)
}

func AddDomains(db *pgxpool.Pool, count int) {
	logger, _ := zap.NewDevelopment()
	client := kju.NewClient(db, logger)

	start := time.Now()
	var queue []*kju.Task
	for i := 0; i != count; i++ {
		data := map[string]string{
			"id":  strconv.Itoa(i),
			"url": fmt.Sprintf("https://www.wikidata.org/wiki/Q%d", i),
		}
		if i%5 == 0 {
			data["divisible"] = "true"
		}
		queue = append(queue, &kju.Task{Handler: "http", Data: data})
		if i%100 == 0 || i == count-1 {
			_, _ = client.QueueTasks(context.TODO(), queue)
			queue = nil
		}
	}
	logger.Info(
		"finished",
		zap.Duration("runtime", time.Now().Sub(start).Round(time.Millisecond)),
		zap.Int("rows", count),
	)
}
