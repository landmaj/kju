package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/landmaj/kju"
	"os"
	"strconv"
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
	client := kju.NewClient(db)

	var queue []struct {
		Name string
		Data map[string]string
	}
	for i := 1; i != count; i++ {
		queue = append(queue, struct {
			Name string
			Data map[string]string
		}{Name: "benchmark", Data: map[string]string{"ID": strconv.Itoa(i)}})
		if i%100 == 0 || i == count {
			_, err := client.QueueTasks(context.TODO(), queue)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Unable to add some task: %v\n", err)
			}
			queue = nil
		}
	}
}
