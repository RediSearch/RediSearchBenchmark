package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
)

func SearchBenchmark(queries []string, idx index.Index, opts interface{}) func() error {

	counter := 0
	return func() error {
		q := query.NewQuery(IndexName, queries[counter%len(queries)]).Limit(0, 1)
		_, _, err := idx.Search(*q)
		counter++
		return err
	}

}

func AutocompleteBenchmark(prefixes []string, ac index.Autocompleter) func() error {
	counter := 0
	return func() error {
		_, err := ac.Suggest(prefixes[counter%len(prefixes)], 5, false)
		counter++
		return err
	}
}
func Benchmark(concurrency int, f func() error) {
	//	queries = []string{"weezer", "germany", "a", "music", "music of the spheres", "abba", "queen",
	//		"nirvana", "benjamin netanyahu", "redis", "redis labs", "german history"} // "computer science", "machine learning"}
	//queries := []string{"earth Though is", "school etc"}
	num := 0
	startTime := time.Now()
	totalTime := time.Duration(0)
	wg := sync.WaitGroup{}
	lck := sync.Mutex{}

	total := 0
	var err error
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			for {
				num++
				total++
				tst := time.Now()

				if err = f(); err != nil {
					panic(err)
				}

				totalTime += time.Since(tst)
				lck.Lock()
				if time.Since(startTime) > time.Second {
					fmt.Println(float64(num)/(float64(time.Since(startTime))/float64(time.Second)), "rps")
					avgLatency := (float64(totalTime) / float64(num)) / float64(time.Millisecond)
					fmt.Printf("Avg latency: %.03fms\n", avgLatency)
					num = 0
					totalTime = 0
					startTime = time.Now()
				}
				lck.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()

}
