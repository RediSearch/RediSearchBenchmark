package main

import (
	"encoding/json"
	"fmt"
	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/RediSearch/RediSearchBenchmark/index"
	"github.com/RediSearch/RediSearchBenchmark/query"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"
)

const (
	CurrentResultFormatVersion = "0.1"
)

// the total time it took to run the functions, to measure average latency, in nanoseconds
var totalTime uint64
var totalOps uint64
var totalHistogram *hdrhistogram.Histogram

// SearchBenchmark returns a closure of a function for the benchmarker to run, using a given index
// and options, on a set of queries
func SearchBenchmark(queries []string, field string, idx index.Index, opts interface{}, debug int) func() error {
	counter := 0
	return func() error {
		q := query.NewQuery(idx.GetName(), queries[counter%len(queries)]).Limit(0, 5).SetField(field)
		_, _, err := idx.FullTextQuerySingleField(*q, debug)
		counter++
		return err
	}
}

// SearchBenchmark returns a closure of a function for the benchmarker to run, using a given index
// and options, on a set of queries
func PrefixBenchmark(terms []string, field string, idx index.Index, prefixMinLen, prefixMaxLen int64, debug int) func() error {
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
		q := query.NewQuery(idx.GetName(), term).Limit(0, 5).SetFlags(query.QueryTypePrefix).SetField(field)
		_, _, err := idx.PrefixQuery(*q, debug)
		counter++
		return err
	}
}

// SearchBenchmark returns a closure of a function for the benchmarker to run, using a given index
// and options, on a set of queries
func WildcardBenchmark(terms []string, field string, idx index.Index, prefixMinLen, prefixMaxLen int64, debug int) func() error {
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
		q := query.NewQuery(idx.GetName(), term).Limit(0, 5).SetFlags(query.QueryTypePrefix).SetField(field)
		_, _, err := idx.WildCardQuery(*q, debug)
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

func GetOverallRatesMap(took time.Duration) map[string]interface{} {
	/////////
	// Overall Rates
	/////////
	configs := map[string]interface{}{}
	overallOpsRate := calculateRateMetrics(totalOps, 0, took)
	configs["overallOpsRate"] = overallOpsRate
	return configs
}

func generateQuantileMap(hist *hdrhistogram.Histogram) (int64, map[string]float64) {
	ops := hist.TotalCount()
	q0 := 0.0
	q50 := 0.0
	q95 := 0.0
	q99 := 0.0
	q999 := 0.0
	q100 := 0.0
	if ops > 0 {
		q0 = float64(hist.ValueAtQuantile(0.0)) / 10e2
		q50 = float64(hist.ValueAtQuantile(50.0)) / 10e2
		q95 = float64(hist.ValueAtQuantile(95.0)) / 10e2
		q99 = float64(hist.ValueAtQuantile(99.0)) / 10e2
		q999 = float64(hist.ValueAtQuantile(99.90)) / 10e2
		q100 = float64(hist.ValueAtQuantile(100.0)) / 10e2
	}

	mp := map[string]float64{"q0": q0, "q50": q50, "q95": q95, "q99": q99, "q999": q999, "q100": q100}
	return ops, mp
}

func GetOverallQuantiles() map[string]interface{} {
	configs := map[string]interface{}{}
	_, all := generateQuantileMap(totalHistogram)
	configs["allCommands"] = all
	return configs
}

// Benchmark runs a given function f for the given duration, and outputs the throughput and latency of the function.
//
// It receives metadata like the engine we are running and the title of the specific benchmark, and writes these along
// with the results to a CSV file given by outfile.
//
// If outfile is "-" we write the result to stdout
func Benchmark(concurrency int, duration time.Duration, instantMutex *sync.Mutex, engine, title string, outfile string, reportingPeriod time.Duration, tab *tabwriter.Writer, f func() error) {
	totalHistogram = hdrhistogram.New(1, 1000000, 3)

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
				instantMutex.Lock()
				totalHistogram.RecordValue(time.Since(tst).Microseconds())
				instantMutex.Unlock()
				// update the total requests performed and total time
				atomic.AddUint64(&totalOps, 1)
				atomic.AddUint64(&totalTime, uint64(time.Since(tst)))

			}
			wg.Done()
		}()
	}
	wg.Wait()
	took := endTime.Sub(startTime)
	// keep this due to the \r
	fmt.Println("")
	log.Println(fmt.Sprintf("Finished the benchmark after %s.", took.String()))

	testResult := TestResult{
		Metadata:            "",
		ResultFormatVersion: CurrentResultFormatVersion,
		Limit:               0,
		Workers:             uint(concurrency),
		MaxRps:              -1,
		DBSpecificConfigs:   nil,
		StartTime:           startTime.Unix() * 1000,
		EndTime:             endTime.Unix() * 1000,
		DurationMillis:      took.Milliseconds(),
		Totals:              nil,
		OverallRates:        GetOverallRatesMap(took),
		OverallQuantiles:    GetOverallQuantiles(),
		TimeSeries:          nil,
	}
	if strings.Compare(outfile, "") != 0 {
		log.Println(fmt.Sprintf("Storing the benchmark results in %s", outfile))
		file, err := json.MarshalIndent(testResult, "", " ")
		if err != nil {
			log.Fatal(err)
		}

		err = ioutil.WriteFile(outfile, file, 0644)
		if err != nil {
			log.Fatal(err)
		}
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
		histogramMutex.Lock()
		instantP50 := float64(totalHistogram.ValueAtQuantile(50.0)) / 10e2
		histogramMutex.Unlock()
		fmt.Printf("%25.0fs %7s %25.2f %25.3f %25d", time.Since(start).Seconds(), completionPercentStr, opsRate, instantP50, currentCount)
		fmt.Printf("\r")
		prevTotalOps = (currentCount)
		prevTime = now
	}
}
