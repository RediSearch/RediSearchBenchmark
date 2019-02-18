package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RediSearch/RediSearchBenchmark/index"
	"github.com/RediSearch/RediSearchBenchmark/query"
)

// SearchBenchmark returns a closure of a function for the benchmarker to run, using a given index
// and options, on a set of queries
func SearchBenchmark(queries []string, idx index.Index, opts interface{}) func() error {

	counter := 0
	return func() error {
		q := query.NewQuery(IndexName, queries[counter%len(queries)]).Limit(0, 5)
		_, _, err := idx.Search(*q)
		counter++
		return err
	}

}

// AutocompleteBenchmark returns a configured autocomplete benchmarking function to be run by
// the benchmarker
func AutocompleteBenchmark(ac index.Autocompleter, fuzzy bool) func() error {
	counter := 0
	sz := len(prefixes)
	return func() error {
		_, err := ac.Suggest(prefixes[rand.Intn(sz)], 5, fuzzy)
		counter++
		return err
	}
}

// Benchmark runs a given function f for the given duration, and outputs the throughput and latency of the function.
//
// It receives metadata like the engine we are running and the title of the specific benchmark, and writes these along
// with the results to a CSV file given by outfile.
//
// If outfile is "-" we write the result to stdout
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

	startTime := time.Now()
	// the total time it took to run the functions, to measure average latency, in nanoseconds
	var totalTime uint64
	var total uint64
	wg := sync.WaitGroup{}

	end := time.Now().Add(duration)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			for time.Now().Before(end) {

				tst := time.Now()

				if err = f(); err != nil {
					panic(err)
				}

				// update the total requests performed and total time
				atomic.AddUint64(&total, 1)
				atomic.AddUint64(&totalTime, uint64(time.Since(tst)))

			}
			wg.Done()
		}()
	}
	wg.Wait()

	avgLatency := (float64(totalTime) / float64(total)) / float64(time.Millisecond)
	rate := float64(total) / (float64(time.Since(startTime)) / float64(time.Second))

	// Output the results to CSV
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
