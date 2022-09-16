package main

import (
	"flag"
	"fmt"
	"math/rand"
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

const (
	elasticUrl                                 = "es.hosts.list"
	elasticUrlDefault                          = "http://127.0.0.1:9200"
	elasticInsecureSSLProp                     = "es.insecure.ssl"
	elasticInsecureSSLPropDefault              = false
	elasticShardCountProp                      = "es.number_of_shards"
	elasticShardCountPropDefault               = 1
	elasticReplicaCountProp                    = "es.number_of_replicas"
	elasticReplicaCountPropDefault             = 0
	elasticUsername                            = "es.username"
	elasticUsernameDefault                     = "elastic"
	elasticPassword                            = "es.password"
	elasticPasswordPropDefault                 = ""
	elasticFlushInterval                       = "es.flush_interval"
	bulkIndexerNumberOfWorkers                 = "es.bulk.num_workers"
	elasticMaxRetriesProp                      = "es.max_retires"
	elasticMaxRetriesPropDefault               = 10
	bulkIndexerFlushBytesProp                  = "es.bulk.flush_bytes"
	bulkIndexerFlushBytesDefault               = 5e+6
	bulkIndexerFlushIntervalSecondsProp        = "es.bulk.flush_interval_secs"
	bulkIndexerFlushIntervalSecondsPropDefault = 30
	elasticIndexNameDefault                    = "ycsb"
	elasticIndexName                           = "es.index"
)

// IndexName is the name of our index on all engines
const IndexNamePrefix = "rd"
const EN_WIKI_DATASET = "enwiki"
const PMC_DATASET = "pmc"
const REDDIT_DATASET = "reddit"
const DEFAULT_DATASET = EN_WIKI_DATASET
const BENCHMARK_SEARCH = "search"
const BENCHMARK_PREFIX = "prefix"
const BENCHMARK_WILDCARD = "wildcard"
const BENCHMARK_SUGGEST = "suggest"
const BENCHMARK_DEFAULT = BENCHMARK_SEARCH
const ENGINE_REDIS = "redis"
const ENGINE_ELASTIC = "elastic"
const ENGINE_SOLR = "solr"
const ENGINE_DEFAULT = ENGINE_REDIS

var indexMetadata = index.NewMetadata().
	AddField(index.NewTextField("body", 1)).
	AddField(index.NewTextField("title", 5)).
	AddField(index.NewTextField("url", 5))

// selectIndex selects and configures the index we are now running based on the engine name, hosts and number of shards
func selectIndex(engine string, hosts []string, user, pass string, temporary int, disableCache bool, name string, cmdPrefix string) (index.Index, index.Autocompleter, interface{}) {

	switch engine {
	case "redis":
		indexMetadata.Options = redisearch.IndexingOptions{Prefix: cmdPrefix}
		idx := redisearch.NewIndex(hosts, pass, temporary, name, indexMetadata)
		ac := redisearch.NewAutocompleter(hosts[0], "ac")
		return idx, ac, query.QueryVerbatim

	case "elastic":
		idx, err := elastic.NewIndex(hosts[0], name, "doc", disableCache, indexMetadata, user, pass)
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
	defaultStopWords := "a, an, and, are, as, at, be, but, by, for, if, in, into, is, it, no, not, of, on, or, such, that, the, their, then, there, these, they, this, to, was, will, with"
	defaultStopWords = strings.Replace(defaultStopWords, " ", "", -1)
	hosts := flag.String("hosts", "localhost:6379", "comma separated list of host:port to redis nodes")
	fileName := flag.String("file", "", "Input file to ingest data from (wikipedia abstracts)")
	dirName := flag.String("dir", "", "Recursively read all files in a directory")
	fileMatch := flag.String("match", ".*", "When reading directories, match only files with this glob")

	//scoreFile := flag.String("scores", "", "read scores of documents CSV for indexing")
	engine := flag.String("engine", ENGINE_DEFAULT, fmt.Sprintf("The search backend to run. One of: [%s]", strings.Join([]string{ENGINE_REDIS, ENGINE_ELASTIC, ENGINE_SOLR}, "|")))
	termsProperty := flag.String("terms-property", "body", "When we read the terms from the input file we read the text from the property specified in this option. If empty the default property field will be used. Default on 'enwiki' dataset = 'body'. Default on 'reddit' dataset = 'body'")
	totalTerms := flag.Int("distinct-terms", 100000, "When reading terms from input files how many terms should be read.")
	randomSeed := flag.Int64("seed", 12345, "PRNG seed.")
	termStopWords := flag.String("stopwords", defaultStopWords, "filtered stopwords for term creation")
	dataset := flag.String("dataset", DEFAULT_DATASET, fmt.Sprintf("The dataset tp process. One of: [%s]", strings.Join([]string{EN_WIKI_DATASET, REDDIT_DATASET, PMC_DATASET}, "|")))
	benchmark := flag.String("benchmark", "", fmt.Sprintf("The benchmark to run. One of: [%s]. If empty will not run.", strings.Join([]string{BENCHMARK_SEARCH, BENCHMARK_PREFIX, BENCHMARK_WILDCARD}, "|")))
	random := flag.Int("random", 0, "Generate random documents with terms like term0..term{N}")
	indexesAmount := flag.Int("indexes", 1, "number of indexes to generate")
	// fuzzy := flag.Bool("fuzzy", false, "For redis only - benchmark fuzzy auto suggest")
	disableCache := flag.Bool("disableCache", false, "for elastic only, disabling query cache")
	seconds := flag.Int("duration", 60, "number of seconds to run the benchmark")
	temporary := flag.Int("temporary", -1, "for redisearch only, create a temporary index that will expire after the given amount of seconds, -1 mean no temporary")
	conc := flag.Int("c", 4, "benchmark concurrency")
	maxDocPerIndex := flag.Int("maxdocs", -1, "specify the numebr of max docs per index, -1 for no limit")
	qs := flag.String("queries", "", "comma separated list of queries to benchmark. Use this option only for the historical reasons via `-queries='barack obama'`. If you don't specify a value it will read the input file and randomize the input search terms")
	outfile := flag.String("o", "benchmark.csv", "results output file. set to - for stdout")
	cmdPrefix := flag.String("prefix", "FT", "Command prefix for FT module")
	password := flag.String("password", "", "database password")
	user := flag.String("user", "", "database username. If empty will use the default for each of the databases")

	flag.Parse()
	rand.Seed(*randomSeed)
	duration := time.Second * time.Duration(*seconds)
	servers := strings.Split(*hosts, ",")
	if len(servers) == 0 {
		panic("No servers given")
	}
	username := ""
	if *user == "" {
		if *engine == "elastic" {
			username = "elastic"
		}
	}

	indexes := make([]index.Index, *indexesAmount)
	var opts interface{}
	var queries []string
	var err error

	if *engine == "redis" {
		opts = query.QueryVerbatim
	}
	// select index to run
	for i := 0; i < *indexesAmount; i++ {
		name := IndexNamePrefix + strconv.Itoa(i)
		idx, _, _ := selectIndex(*engine, servers, username, *password, *temporary, *disableCache, name, *cmdPrefix)
		indexes[i] = idx
	}

	if *qs == "" && *benchmark != "" {
		fmt.Println("Using input file to produce terms for the benchmarks")
		if *dataset == EN_WIKI_DATASET {
			wr := &ingest.WikipediaAbstractsReader{}
			if *fileName != "" {
				if queries, err = ingest.ReadTerms(*fileName, wr, indexes[0], 0, 10000, *totalTerms, *termsProperty, strings.Split(*termStopWords, ",")); err != nil {
					panic(fmt.Sprintf("Failed on Term preparation due to %v", err))
				}
			}
		} else if *dataset == PMC_DATASET {
			panic("not yet implemented")
		}
	} else {
		queries = strings.Split(*qs, ",")
	}

	if len(queries) == 0 && *benchmark != "" {
		panic("you've specified a benchmark but query terms are empty")
	}
	if *benchmark != "" {
		fmt.Println(fmt.Sprintf("Using a total of %d distinct input terms for benchmark queries", len(queries)))
	}

	// Search benchmark
	if *benchmark == BENCHMARK_SEARCH {
		if *indexesAmount > 1 {
			panic("search not supported on multiple indexes!!!")
		}
		name := fmt.Sprintf("search: %s", *qs)
		Benchmark(*conc, duration, *engine, name, *outfile, SearchBenchmark(queries, indexes[0], opts))
		os.Exit(0)
	}

	// Search benchmark
	if *benchmark == BENCHMARK_SEARCH {
		if *indexesAmount > 1 {
			panic("search not supported on multiple indexes!!!")
		}
		name := fmt.Sprintf("search: %s", *qs)
		Benchmark(*conc, duration, *engine, name, *outfile, SearchBenchmark(queries, indexes[0], opts))
		os.Exit(0)
	}

	// Auto-suggest benchmark
	if *benchmark == BENCHMARK_SUGGEST {
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
					if *dataset == EN_WIKI_DATASET {
						wr := &ingest.WikipediaAbstractsReader{}
						if *fileName != "" {

							if err := ingest.ReadFile(*fileName, wr, idx, nil, redisearch.IndexingOptions{}, 1000, *maxDocPerIndex); err != nil {
								panic(err)
							}
						} else if *dirName != "" {
							ingest.ReadDir(*dirName, *fileMatch, wr, idx, nil, redisearch.IndexingOptions{},
								1000, runtime.NumCPU(), 250, nil, *maxDocPerIndex)

						}
					} else if *dataset == REDDIT_DATASET {
						wr := &ingest.RedditReader{}
						if *fileName != "" {
							if err := ingest.ReadFile(*fileName, wr, idx, nil, redisearch.IndexingOptions{}, 1000, *maxDocPerIndex); err != nil {
								panic(err)
							}
						} else if *dirName != "" {
							ingest.ReadDir(*dirName, *fileMatch, wr, idx, nil, redisearch.IndexingOptions{},
								1000, runtime.NumCPU(), 250, nil, *maxDocPerIndex)
						}
					} else if *dataset == PMC_DATASET {
						panic("not yet implemented")
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
