package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
)

func SearchBenchmark(queries []string, idx index.Index, opts interface{}) func() error {

	counter := 0
	return func() error {
		q := query.NewQuery(IndexName, queries[counter%len(queries)]).Limit(0, 5)
		_, _, err := idx.Search(*q)
		counter++
		return err
	}

}

func AutocompleteBenchmark(ac index.Autocompleter, fuzzy bool) func() error {
	counter := 0
	sz := len(prefixes)
	return func() error {
		_, err := ac.Suggest(prefixes[rand.Intn(sz)], 5, fuzzy)
		counter++
		return err
	}
}
func Benchmark(concurrency int, duration time.Duration, engine, title string, outfile string, f func() error) {

	var out io.WriteCloser
	var err error
	if outfile == "-" {
		out = os.Stdout
	} else {
		out, err = os.OpenFile(outfile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0665)
		if err != nil {
			panic(err)
		}
		defer out.Close()
	}

	//	queries = []string{"weezer", "germany", "a", "music", "music of the spheres", "abba", "queen",
	//		"nirvana", "benjamin netanyahu", "redis", "redis labs", "german history"} // "computer science", "machine learning"}
	//queries := []string{"earth Though is", "school etc"}
	startTime := time.Now()
	totalTime := time.Duration(0)
	wg := sync.WaitGroup{}

	total := 0
	end := time.Now().Add(duration)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			for time.Now().Before(end) {

				tst := time.Now()

				if err = f(); err != nil {
					panic(err)
				}
				total++

				totalTime += time.Since(tst)

			}
			wg.Done()
		}()
	}
	wg.Wait()

	avgLatency := (float64(totalTime) / float64(total)) / float64(time.Millisecond)
	rate := float64(total) / (float64(time.Since(startTime)) / float64(time.Second))

	w := csv.NewWriter(out)

	err = w.Write([]string{engine, title,
		fmt.Sprintf("%d", concurrency),
		fmt.Sprintf("%.02f", rate),
		fmt.Sprintf("%.02f", avgLatency)})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing: %s\n", err)
	} else {
		fmt.Println("Done!")
		w.Flush()
	}

}
