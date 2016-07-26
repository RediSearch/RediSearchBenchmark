package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
)

func Benchmark(queries []string, concurrency int, idx index.Index) {
	//	queries = []string{"weezer", "germany", "a", "music", "music of the spheres", "abba", "queen",
	//		"nirvana", "benjamin netanyahu", "redis", "redis labs", "german history"} // "computer science", "machine learning"}
	//queries := []string{"earth Though is", "school etc"}
	num := 0
	startTime := time.Now()
	totalTime := time.Duration(0)
	wg := sync.WaitGroup{}
	lck := sync.Mutex{}

	total := 0
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			for {
				num++
				total++
				tst := time.Now()
				q := query.NewQuery(IndexName, queries[total%len(queries)]).Limit(0, 1)
				_, _, err := idx.Search(*q)
				if err != nil {
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
