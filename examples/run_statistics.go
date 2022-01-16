package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/hpcloud/tail"
	"github.com/montanaflynn/stats"
)

const maxPause = time.Second

type Log struct {
	TimeLifecycle *float64 `json:"time_lifecycle"`
	TimeQueue     *float64 `json:"time_queue"`
	TimeExecution *float64 `json:"time_execution"`
}

func main() {
	t, err := tail.TailFile("log.json", tail.Config{Follow: true})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to tail file: %v\n", err)
		os.Exit(1)
	}

	m := sync.Mutex{}
	statistics := map[string][]float64{}
	lastLine := time.Now()
	printed := true

	go func() {
		for {
			if time.Now().Sub(lastLine) > maxPause && !printed {
				m.Lock()

				writer := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', tabwriter.AlignRight)
				_, _ = fmt.Fprintf(writer, "Execution time:\t%s", prepareStats(statistics["time_execution"], time.Nanosecond))
				_, _ = fmt.Fprintf(writer, "Time in queue:\t%s", prepareStats(statistics["time_queue"], time.Millisecond))
				_, _ = fmt.Fprintf(writer, "Lifetime:\t%s", prepareStats(statistics["time_lifecycle"], time.Second))
				_ = writer.Flush()

				statistics = map[string][]float64{}
				printed = true
				m.Unlock()
			}
			time.Sleep(time.Millisecond * 100)
		}
	}()

	for line := range t.Lines {
		var log Log
		_ = json.Unmarshal([]byte(line.Text), &log)
		if log.TimeLifecycle != nil {
			m.Lock()
			if printed {
				fmt.Println("\nCOLLECTING DATA")
			}
			statistics["time_lifecycle"] = append(statistics["time_lifecycle"], *log.TimeLifecycle)
			statistics["time_queue"] = append(statistics["time_queue"], *log.TimeQueue)
			statistics["time_execution"] = append(statistics["time_execution"], *log.TimeExecution)
			printed = false
			lastLine = time.Now()
			m.Unlock()
		}
	}
}

func prepareStats(data []float64, round time.Duration) string {
	mean, _ := stats.Mean(data)
	percentile95, _ := stats.Percentile(data, 95)
	percentile99, _ := stats.Percentile(data, 99)
	return fmt.Sprintf(
		"[mean: %s]\t[95th percentile: %s]\t[99th percentile: %s]\n",
		time.Duration(mean*float64(time.Second)).Round(round),
		time.Duration(percentile95*float64(time.Second)).Round(round),
		time.Duration(percentile99*float64(time.Second)).Round(round),
	)
}
