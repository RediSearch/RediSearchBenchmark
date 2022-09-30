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

func SuffixBenchmark(terms []string, field string, idx index.Index, prefixMinLen, prefixMaxLen int64, debug int) func() error {
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
		term = "*" + term[len(term)-int(prefixSize):]
		q := query.NewQuery(idx.GetName(), term).Limit(0, 5).SetFlags(query.QueryTypeSuffix).SetField(field)
		_, _, err := idx.SuffixQuery(*q, debug)
		counter++
		return err
	}
}

func ContainsBenchmark(terms []string, field string, idx index.Index, prefixMinLen, prefixMaxLen int64, debug int) func() error {
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
		term = "*" + term + "*"

		q := query.NewQuery(idx.GetName(), term).Limit(0, 5).SetField(field)
		_, _, err := idx.ContainsQuery(*q, debug)
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
	return func() error {
		term := terms[counter%len(terms)]
		var prefixSize int64 = prefixMinLen
		n := rand.Int63n(int64(prefixMaxLen - prefixMinLen))
		prefixSize = prefixSize + n
		wildcardPos := prefixSize + 1
		minTermLen := wildcardPos + 1
		for minTermLen > int64(len(term)) {
			counter++
			term = terms[counter%len(terms)]
		}
		term = term[0:minTermLen]
		term = term[:prefixSize] + "*" + term[minTermLen-1:]
		q := query.NewQuery(idx.GetName(), term).Limit(0, 5).SetField(field)
		_, _, err := idx.WildCardQuery(*q, debug)
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
func Benchmark(concurrency int, duration time.Duration, instantMutex *sync.Mutex, engine, title string, outfile string, reportingPeriod time.Duration, tab *tabwriter.Writer, f func() error) {
	totalHistogram = hdrhistogram.New(1, 1000000000, 3)

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
				err = totalHistogram.RecordValue(time.Since(tst).Microseconds())
				if err != nil {
					panic(err)
				}
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
