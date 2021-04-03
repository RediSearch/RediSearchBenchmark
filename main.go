package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"runtime"

	"sync"

	"github.com/RediSearch/RediSearchBenchmark/index"
	"github.com/RediSearch/RediSearchBenchmark/index/elastic"
	"github.com/RediSearch/RediSearchBenchmark/index/redisearch"
	"github.com/RediSearch/RediSearchBenchmark/index/solr"
	"github.com/RediSearch/RediSearchBenchmark/ingest"
	"github.com/RediSearch/RediSearchBenchmark/query"
	"github.com/RediSearch/RediSearchBenchmark/synth"
)

// IndexName is the name of our index on all engines
const IndexNamePrefix = "rd"

var indexMetadata = index.NewMetadata().
	AddField(index.NewTextField("body", 1)).
	AddField(index.NewTextField("title", 5)).
	AddField(index.NewTextField("url", 5))
	//AddField(index.NewNumericField("ups"))

// selectIndex selects and configures the index we are now running based on the engine name, hosts and number of shards
func selectIndex(engine string, hosts []string, pass string, temporary int, disableCache bool, name string, cmdPrefix string) (index.Index, index.Autocompleter, interface{}) {

	switch engine {
	case "redis":
		indexMetadata.Options = redisearch.IndexingOptions{Prefix: cmdPrefix}
		idx := redisearch.NewIndex(hosts, pass, temporary, name, indexMetadata)
		ac := redisearch.NewAutocompleter(hosts[0], "ac")
		return idx, ac, query.QueryVerbatim

	case "elastic":
		idx, err := elastic.NewIndex(hosts[0], name, "doc", disableCache, indexMetadata)
		if err != nil {
			panic(err)
		}
		return idx, idx, 0
	case "solr":
		idx, err := solr.NewIndex(hosts[0], name, indexMetadata)
		if err != nil {
			panic(err)
		}
		return idx, idx, 0

	}
	panic("could not find index type " + engine)
}

func main() {

	hosts := flag.String("hosts", "localhost:6379", "comma separated list of host:port to redis nodes")
	fileName := flag.String("file", "", "Input file to ingest data from (wikipedia abstracts)")
	dirName := flag.String("dir", "", "Recursively read all files in a directory")
	fileMatch := flag.String("match", ".*", "When reading directories, match only files with this glob")

	//scoreFile := flag.String("scores", "", "read scores of documents CSV for indexing")
	engine := flag.String("engine", "redis", "The search backend to run")
	benchmark := flag.String("benchmark", "", "[search|suggest] - if set, we run the given benchmark")
	random := flag.Int("random", 0, "Generate random documents with terms like term0..term{N}")
	indexesAmount := flag.Int("indexes", 1, "number of indexes to generate")
	// fuzzy := flag.Bool("fuzzy", false, "For redis only - benchmark fuzzy auto suggest")
	disableCache := flag.Bool("disableCache", false, "for elastic only, disabling query cache")
	seconds := flag.Int("duration", 5, "number of seconds to run the benchmark")
	temporary := flag.Int("temporary", -1, "for redisearch only, create a temporary index that will expire after the given amount of seconds, -1 mean no temporary")
	conc := flag.Int("c", 4, "benchmark concurrency")
	maxDocPerIndex := flag.Int("maxdocs", -1, "specify the numebr of max docs per index, -1 for no limit")
	qs := flag.String("queries", "hello world", "comma separated list of queries to benchmark")
	outfile := flag.String("o", "benchmark.csv", "results output file. set to - for stdout")
	cmdPrefix := flag.String("prefix", "FT", "Command prefix for FT module")
	password := flag.String("password", "", "redis database password")

	flag.Parse()
	duration := time.Second * time.Duration(*seconds)
	servers := strings.Split(*hosts, ",")
	if len(servers) == 0 {
		panic("No servers given")
	}
	queries := strings.Split(*qs, ",")

	indexes := make([]index.Index, *indexesAmount)
	var opts interface{}
	if *engine == "redis" {
		opts = query.QueryVerbatim
	}
	// select index to run
	for i := 0; i < *indexesAmount; i++ {
		name := IndexNamePrefix + strconv.Itoa(i)
		idx, _, _ := selectIndex(*engine, servers, *password, *temporary, *disableCache, name, *cmdPrefix)
		indexes[i] = idx
	}

	// Search benchmark
	if *benchmark == "search" {
		if *indexesAmount > 1 {
			panic("search not supported on multiple indexes!!!")
		}
		name := fmt.Sprintf("search: %s", *qs)
		Benchmark(*conc, duration, *engine, name, *outfile, SearchBenchmark(queries, indexes[0], opts))
		os.Exit(0)
	}

	// Auto-suggest benchmark
	if *benchmark == "suggest" {
		panic("not supported!!")
		// Benchmark(*conc, duration, *engine, "suggest", *outfile, AutocompleteBenchmark(ac, *fuzzy))
		// os.Exit(0)
	}

	// ingest random documents
	if *random > 0 {
		indexes[0].Drop()
		err := indexes[0].Create()
		if err != nil {
			panic(err)
		}

		N := 1000
		gen := synth.NewDocumentGenerator(*random, map[string][2]int{"title": {5, 10}, "body": {10, 20}})
		chunk := make([]index.Document, N)
		n := 0
		ch := make(chan index.Document, N)
		go func() {
			for i := 0; i < *maxDocPerIndex || *maxDocPerIndex == -1; i++ {
				ch <- gen.Generate(0)
			}
		}()
		for {

			for i := 0; i < N; i++ {
				chunk[i] = <-ch
				n++
			}

			indexes[0].Index(chunk, nil)
			fmt.Println(n)
		}

	}
	// ingest documents into the selected engine
	if (*fileName != "" || *dirName != "") && *benchmark == "" {

		var wg sync.WaitGroup
		idxChan := make(chan index.Index, 1)
		for i := 0; i < 30; i++ {
			wg.Add(1)
			go func(idxChan chan index.Index) {
				defer wg.Done()
				for idx := range idxChan {
					idx.Drop()
					err := idx.Create()
					if err != nil {
						panic(err)
					}
					wr := &ingest.WikipediaAbstractsReader{}

					if *fileName != "" {

						if err := ingest.ReadFile(*fileName, wr, idx, nil, redisearch.IndexingOptions{}, 1000, *maxDocPerIndex); err != nil {
							panic(err)
						}
					} else if *dirName != "" {
						ingest.ReadDir(*dirName, *fileMatch, wr, idx, nil, redisearch.IndexingOptions{},
							1000, runtime.NumCPU(), 250, nil, *maxDocPerIndex)

					}
				}
			}(idxChan)
		}

		for _, idx := range indexes {
			idxChan <- idx
		}
		close(idxChan)
		wg.Wait()
		os.Exit(0)
	}

	fmt.Fprintln(os.Stderr, "No benchmark or input file specified")
	flag.Usage()
	os.Exit(-1)
}
