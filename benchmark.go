package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/RediSearch/RediSearchBenchmark/index"
	"github.com/RediSearch/RediSearchBenchmark/query"
)

// the total time it took to run the functions, to measure average latency, in nanoseconds
var totalTime uint64
var totalOps uint64

// SearchBenchmark returns a closure of a function for the benchmarker to run, using a given index
// and options, on a set of queries
func SearchBenchmark(queries []string, idx index.Index, opts interface{}) func() error {
	counter := 0
	return func() error {
		q := query.NewQuery(idx.GetName(), queries[counter%len(queries)]).Limit(0, 5)
		_, _, err := idx.Search(*q)
		counter++
		return err
	}
}

// SearchBenchmark returns a closure of a function for the benchmarker to run, using a given index
// and options, on a set of queries
func PrefixBenchmark(terms []string, idx index.Index, prefixMinLen, prefixMaxLen int64) func() error {
	counter := 0
	fixedPrefixSize := false
	if prefixMinLen == prefixMaxLen {
		fixedPrefixSize = true
	}
	return func() error {
		term := terms[counter%len(terms)]
		var prefixSize int64 = prefixMinLen
		if !fixedPrefixSize {
			n := rand.Int63n(int64(prefixMaxLen - prefixMinLen))
			prefixSize = prefixSize + n
		}
		for prefixSize > int64(len(term)) {
			counter++
			term = terms[counter%len(terms)]
		}
		term = term[0:prefixSize]
		q := query.NewQuery(idx.GetName(), term).Limit(0, 5).SetFlags(query.QueryTypePrefix)
		_, _, err := idx.PrefixSearch(*q)
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
func Benchmark(concurrency int, duration time.Duration, engine, title string, outfile string, reportingPeriod time.Duration, tab *tabwriter.Writer, f func() error) {

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
	endTime := startTime.Add(duration)
	wg := sync.WaitGroup{}

	if reportingPeriod.Nanoseconds() > 0 {
		go report(reportingPeriod, startTime, endTime, tab)
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			for time.Now().Before(endTime) {

				tst := time.Now()

				if err = f(); err != nil {
					panic(err)
				}

				// update the total requests performed and total time
				atomic.AddUint64(&totalOps, 1)
				atomic.AddUint64(&totalTime, uint64(time.Since(tst)))

			}
			wg.Done()
		}()
	}
	wg.Wait()

	avgLatency := (float64(totalTime) / float64(totalOps)) / float64(time.Millisecond)
	rate := float64(totalOps) / (float64(time.Since(startTime)) / float64(time.Second))

	// Output the results to CSV
	w := csv.NewWriter(out)

	err = w.Write([]string{engine, title,
		fmt.Sprintf("%d", concurrency),
		fmt.Sprintf("%.02f", rate),
		fmt.Sprintf("%.02f", avgLatency)})

	if err != nil {
		log.Fatalf("Error writing: %s\n", err)
	} else {
		log.Println("Done!")
		w.Flush()
	}

}

func calculateRateMetrics(current, prev uint64, took time.Duration) (rate float64) {
	rate = float64(current-prev) / float64(took.Seconds())
	return
}

// report handles periodic reporting of loading stats
func report(period time.Duration, start, end time.Time, w *tabwriter.Writer) {
	prevTime := start
	prevTotalOps := uint64(0)
	totalDuration := end.Sub(start)
	totalDurationMs := float64(totalDuration.Milliseconds())

	fmt.Printf("%26s %7s %25s %25s %25s\n", "Test time", " ", "Command Rate", "Client p50 with RTT(ms)", "Total Commands")
	for now := range time.NewTicker(period).C {

		took := now.Sub(prevTime)
		tookTotal := end.Sub(now)
		currentCount := atomic.LoadUint64(&totalOps)
		completionPercent := (totalDurationMs - float64(tookTotal.Milliseconds())) / totalDurationMs * 100.0
		completionPercentStr := fmt.Sprintf("[%3.1f%%]", completionPercent)

		opsRate := calculateRateMetrics((currentCount), prevTotalOps, took)
		instantP50 := 0.0
		fmt.Printf("%25.0fs %7s %25.2f %25.3f %25d", time.Since(start).Seconds(), completionPercentStr, opsRate, instantP50, currentCount)
		fmt.Printf("\r")
		prevTotalOps = (currentCount)
		prevTime = now
	}
}
