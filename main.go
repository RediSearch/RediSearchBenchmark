package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/index/elastic"
	"github.com/RedisLabs/RediSearchBenchmark/index/redisearch"
	"github.com/RedisLabs/RediSearchBenchmark/index/solr"
	"github.com/RedisLabs/RediSearchBenchmark/ingest"
	"github.com/RedisLabs/RediSearchBenchmark/query"
)

const IndexName = "wik"

var indexMetadata = index.NewMetadata().
	AddField(index.NewTextField("title", 10)).
	AddField(index.NewTextField("body", 1))

func selectIndex(engine string, hosts []string, partitions int) (index.Index, index.Autocompleter, interface{}) {

	switch engine {
	case "redis":
		//return redisearch.NewIndex(hosts[0], "wik{0}", indexMetadata)
		idx := redisearch.NewDistributedIndex(IndexName, hosts, partitions, indexMetadata)
		return idx, idx, query.QueryVerbatim
	case "elastic":
		idx, err := elastic.NewIndex(hosts[0], IndexName, indexMetadata)
		if err != nil {
			panic(err)
		}
		return idx, idx, 0
	case "solr":
		idx, err := solr.NewIndex(hosts[0], IndexName, indexMetadata)
		if err != nil {
			panic(err)
		}
		return idx, idx, 0

	}
	panic("could not find index type " + engine)
}

func main() {

	hosts := flag.String("hosts", "localhost:6379", "comma separated list of host:port to redis nodes")
	partitions := flag.Int("shards", 1, "the number of partitions we want (AT LEAST the number of cluster shards)")
	fileName := flag.String("file", "", "Input file to ingest data from (wikipedia abstracts)")
	scoreFile := flag.String("scores", "", "read scores of documents CSV for indexing")
	engine := flag.String("engine", "redis", "The search backend to run")
	benchmark := flag.String("benchmark", "", "[search|suggest] - if set, we run the given benchmark")
	fuzzy := flag.Bool("fuzzy", false, "For redis only - benchmark fuzzy auto suggest")
	seconds := flag.Int("duration", 5, "number of seconds to run the benchmark")
	conc := flag.Int("c", 4, "benchmark concurrency")
	qs := flag.String("queries", "hello world", "comma separated list of queries to benchmark")
	outfile := flag.String("o", "benchmark.csv", "results output file. set to - for stdout")
	duration := time.Second * time.Duration(*seconds)

	flag.Parse()
	servers := strings.Split(*hosts, ",")
	if len(servers) == 0 {
		panic("No servers given")
	}
	queries := strings.Split(*qs, ",")

	// select index to run
	idx, ac, opts := selectIndex(*engine, servers, *partitions)

	if *benchmark == "search" {
		name := fmt.Sprintf("search: %s", *qs)
		Benchmark(*conc, duration, *engine, name, *outfile, SearchBenchmark(queries, idx, opts))
	} else if *benchmark == "suggest" {
		Benchmark(*conc, duration, *engine, "suggest", *outfile, AutocompleteBenchmark(ac, *fuzzy))
	} else if *fileName != "" && *benchmark == "" {
		if ac != nil {
			ac.Delete()
		}

		idx.Drop()
		idx.Create()
		wr := ingest.NewWikipediaAbstractsReader()

		if *scoreFile != "" {
			if err := wr.LoadScores(*scoreFile); err != nil {
				panic(err)
			}
		}

		if err := ingest.IngestDocuments(*fileName, wr, idx, ac, nil, 50000); err != nil {
			panic(err)
		}
	}
	//	//LoadWikipedia(*fileName, modIdx, store)
	//	if *benchmark {

	//		f, err := os.Create("prof.out")
	//		if err != nil {
	//			log.Fatal(err)
	//		}
	//		pprof.StartCPUProfile(f)
	//		defer pprof.StopCPUProfile()

	//		Benchmark(queries, *conc, modIdx, store)
	//	}

}
