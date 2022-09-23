package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
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
	// IndexName is the name of our index on all engines
	IndexNamePrefix             = "rd"
	EN_WIKI_DATASET             = "enwiki"
	PMC_DATASET                 = "pmc"
	REDDIT_DATASET              = "reddit"
	DEFAULT_DATASET             = EN_WIKI_DATASET
	BENCHMARK_SEARCH            = "search"
	BENCHMARK_SEARCH_MULTIMATCH = "search:multi_match"
	BENCHMARK_PREFIX            = "prefix"
	BENCHMARK_WILDCARD          = "wildcard"
	BENCHMARK_SUGGEST           = "suggest"
	BENCHMARK_DEFAULT           = BENCHMARK_SEARCH
	ENGINE_REDIS                = "redis"
	ENGINE_ELASTIC              = "elastic"
	ENGINE_SOLR                 = "solr"
	TERM_QUERY_MAX_LEN          = "term-query-prefix-max-len"
	ENGINE_DEFAULT              = ENGINE_REDIS
)

// this mutex does not affect any of the client go-routines ( it's only to sync between main thread and datapoints processer go-routines )
var histogramMutex sync.Mutex

var indexMetadataEnWiki = index.NewMetadata().
	AddField(index.NewTextField("body", 1)).
	AddField(index.NewTextField("title", 1)).
	AddField(index.NewTextField("url", 1))

//1) "accession"
//2) "journal"
//3) "name"
//4) "timestamp"
//5) "date"
//6) "volume"
//7) "pmid"
//8) "body"
//9) "issue"

var indexMetadataPMC = index.NewMetadata().
	AddField(index.NewTextField("accession", 1)).
	AddField(index.NewTextField("journal", 1)).
	AddField(index.NewTextField("name", 1)).
	AddField(index.NewNumericField("timestamp")).
	AddField(index.NewTextField("date", 1)).
	AddField(index.NewTextField("volume", 1)).
	AddField(index.NewTextField("pmid", 1)).
	AddField(index.NewTextField("body", 1)).
	AddField(index.NewTextField("issue", 1))

// selectIndex selects and configures the index we are now running based on the engine name, hosts and number of shards
func selectIndex(indexMetadata *index.Metadata, engine string, hosts []string, user, pass string, temporary int, disableCache bool, name string, cmdPrefix string, shardCount, replicaCount, indexerNumCPUs int) (index.Index, index.Autocompleter, interface{}) {

	switch engine {
	case ENGINE_REDIS:
		indexMetadata.Options = redisearch.IndexingOptions{Prefix: cmdPrefix}
		idx := redisearch.NewIndex(hosts, pass, temporary, name, indexMetadata)
		ac := redisearch.NewAutocompleter(hosts[0], "ac")
		return idx, ac, query.QueryVerbatim
	case ENGINE_ELASTIC:
		idx, err := elastic.NewIndex(hosts[0], name, "doc", disableCache, indexMetadata, user, pass, shardCount, replicaCount, indexerNumCPUs)
		if err != nil {
			panic(err)
		}
		return idx, idx, 0
	case ENGINE_SOLR:
		idx, err := solr.NewIndex(hosts[0], name, indexMetadata)
		if err != nil {
			panic(err)
		}
		return idx, idx, 0

	}
	panic("could not find index type " + engine)
}

func main() {
	runtimeCPUs := runtime.NumCPU()
	defaultStopWords := "a, an, and, are, as, at, be, but, by, for, if, in, into, is, it, no, not, of, on, or, such, that, the, their, then, there, these, they, this, to, was, will, with"
	defaultStopWords = strings.Replace(defaultStopWords, " ", "", -1)
	hosts := flag.String("hosts", "localhost:6379", "comma separated list of host:port to redis nodes")
	fileName := flag.String("file", "", "Input file to ingest data from (wikipedia abstracts)")
	dirName := flag.String("dir", "", "Recursively read all files in a directory")
	fileMatch := flag.String("match", ".*", "When reading directories, match only files with this glob")
	engine := flag.String("engine", ENGINE_DEFAULT, fmt.Sprintf("The search backend to run. One of: [%s]", strings.Join([]string{ENGINE_REDIS, ENGINE_ELASTIC, ENGINE_SOLR}, "|")))
	termsProperty := flag.String("terms-property", "body", "When we read the terms from the input file we read the text from the property specified in this option. If empty the default property field will be used. Default on 'enwiki' dataset = 'body'. Default on 'reddit' dataset = 'body'")
	termQueryPrefixMinLen := flag.Int64("term-query-prefix-min-len", 3, "Minimum prefix length for the generated term queries.")
	termQueryPrefixMaxLen := flag.Int64(TERM_QUERY_MAX_LEN, 3, "Maximum prefix length for the generated term queries.")
	totalTerms := flag.Int("distinct-terms", 100000, "When reading terms from input files how many terms should be read.")
	queryField := flag.String("benchmark-query-fieldname", "", "fieldname to use for search|prefix|wildcard benchmarks. If empty will use the default per dataset.")
	randomSeed := flag.Int64("seed", 12345, "PRNG seed.")
	termStopWords := flag.String("stopwords", defaultStopWords, "filtered stopwords for term creation")
	dataset := flag.String("dataset", DEFAULT_DATASET, fmt.Sprintf("The dataset tp process. One of: [%s]", strings.Join([]string{EN_WIKI_DATASET, REDDIT_DATASET, PMC_DATASET}, "|")))
	benchmark := flag.String("benchmark", "", fmt.Sprintf("The benchmark to run. One of: [%s]. If empty will not run.", strings.Join([]string{BENCHMARK_SEARCH, BENCHMARK_PREFIX, BENCHMARK_WILDCARD}, "|")))
	random := flag.Int("random", 0, "Generate random documents with terms like term0..term{N}")
	indexesAmount := flag.Int("indexes", 1, "number of indexes to generate")
	elasticShardCount := flag.Int("es.number_of_shards", 1, "elastic shard count")
	elasticReplicaCount := flag.Int("es.number_of_replicas", 0, "elastic replica count")
	elasticEnableCache := flag.Bool("es.requests.cache.enable", true, "for elastic only. enable query cache.")
	verbatimEnabled := flag.Bool("verbatim", false, "for redisearch only. does not try to use stemming for query expansion but searches the query terms verbatim.")
	seconds := flag.Int("duration", 60, "number of seconds to run the benchmark")
	temporary := flag.Int("temporary", -1, "for redisearch only, create a temporary index that will expire after the given amount of seconds, -1 mean no temporary")
	conc := flag.Int("c", runtimeCPUs, "benchmark concurrency")
	debugLevel := flag.Int("debug-level", 0, "print debug info according to debug level. If 0 disabled.")
	maxDocPerIndex := flag.Int("maxdocs", -1, "specify the number of max docs per index, -1 for no limit")
	qs := flag.String("queries", "", "comma separated list of queries to benchmark. Use this option only for the historical reasons via `-queries='barack obama'`. If you don't specify a value it will read the input file and randomize the input search terms")
	outfile := flag.String("o", "benchmark.json", "results output file. set to - for stdout")
	cmdPrefix := flag.String("redis.cmd.prefix", "FT", "Command prefix for FT module")
	password := flag.String("password", "", "database password")
	user := flag.String("user", "", "database username. If empty will use the default for each of the databases")
	reportingPeriod := flag.Duration("reporting-period", 1*time.Second, "Period to report runtime stats")

	benchmarkQueryField := *queryField
	if benchmarkQueryField == "" {
		switch *dataset {
		case EN_WIKI_DATASET:
			benchmarkQueryField = "body"
		case PMC_DATASET:
			benchmarkQueryField = "body"
		}
	}

	flag.Parse()
	rand.Seed(*randomSeed)
	duration := time.Second * time.Duration(*seconds)
	servers := strings.Split(*hosts, ",")
	if len(servers) == 0 {
		panic("No servers given")
	}
	username := *user
	if username == "" {
		if (*engine) == ENGINE_ELASTIC {
			username = "elastic"
		}
	}

	indexes := make([]index.Index, *indexesAmount)
	var opts interface{}
	var queries []string
	var err error
	var indexMetadata *index.Metadata
	if *dataset == EN_WIKI_DATASET {
		indexMetadata = indexMetadataEnWiki
	} else if *dataset == PMC_DATASET {
		indexMetadata = indexMetadataPMC
	}

	log.Printf("Using a total of %d concurrent benchmark workers", *conc)

	if *engine == "redis" && *verbatimEnabled {
		log.Println("Enabling VERBATIM mode on FullTextQuerySingleField benchmarks.")
		opts = query.QueryVerbatim
	}
	// select index to run
	for i := 0; i < *indexesAmount; i++ {
		name := IndexNamePrefix + strconv.Itoa(i)
		idx, _, _ := selectIndex(indexMetadata, *engine, servers, username, *password, *temporary, !*elasticEnableCache, name, *cmdPrefix, *elasticShardCount, *elasticReplicaCount, *conc)
		indexes[i] = idx
	}

	if *qs == "" && *benchmark != "" {
		log.Println("Using input file to produce terms for the benchmarks")
		if *dataset == EN_WIKI_DATASET {
			wr := &ingest.WikipediaAbstractsReader{}
			if *fileName != "" {
				if queries, err = ingest.ReadTerms(*fileName, wr, indexes[0], 0, 10000, *totalTerms, *termsProperty, strings.Split(*termStopWords, ",")); err != nil {
					log.Fatalf("Failed on Term preparation due to %v", err)
				}
			}
		} else if *dataset == PMC_DATASET {
			wr := &ingest.PmcReader{}
			if *fileName != "" {
				if queries, err = ingest.ReadTerms(*fileName, wr, indexes[0], 0, 10000, *totalTerms, *termsProperty, strings.Split(*termStopWords, ",")); err != nil {
					log.Fatalf("Failed on Term preparation due to %v", err)
				}
			}
		}
	} else {
		queries = strings.Split(*qs, ",")
	}

	if len(queries) == 0 && *benchmark != "" {
		panic("you've specified a benchmark but query terms are empty")
	}
	if *benchmark != "" {
		log.Println(fmt.Sprintf("Using a total of %d distinct input terms for benchmark queries", len(queries)))
	}
	w := new(tabwriter.Writer)
	w.Init(os.Stderr, 20, 0, 0, ' ', tabwriter.AlignRight)
	// wildcard benchmark
	if *benchmark == BENCHMARK_WILDCARD {
		if *indexesAmount > 1 {
			panic("search not supported on multiple indexes!")
		}
		name := fmt.Sprintf("wildcard: %d terms", len(queries))
		log.Println("Starting term-level queries benchmark: Type WILDCARD")
		prefixMaxLen := *termQueryPrefixMaxLen
		if (prefixMaxLen - 2) <= *termQueryPrefixMinLen {
			prefixMaxLen = prefixMaxLen + 2
			log.Println(fmt.Sprintf("%s needs to be at least larger by 2 than min length given we want the wildcard to be present at the midle of the term. Forcing %s=%d", TERM_QUERY_MAX_LEN, TERM_QUERY_MAX_LEN, prefixMaxLen))
		}
		Benchmark(*conc, duration, &histogramMutex, *engine, name, *outfile, *reportingPeriod, w, WildcardBenchmark(queries, benchmarkQueryField, indexes[0], *termQueryPrefixMinLen, prefixMaxLen, *debugLevel))
		os.Exit(0)
	}
	// prefix benchmark
	if *benchmark == BENCHMARK_PREFIX {
		if *indexesAmount > 1 {
			panic("search not supported on multiple indexes!")
		}
		name := fmt.Sprintf("prefix: %d terms", len(queries))
		log.Println("Starting term-level queries benchmark: Type PREFIX")
		Benchmark(*conc, duration, &histogramMutex, *engine, name, *outfile, *reportingPeriod, w, PrefixBenchmark(queries, benchmarkQueryField, indexes[0], *termQueryPrefixMinLen, *termQueryPrefixMaxLen, *debugLevel))
		os.Exit(0)
	}

	// FullTextQuerySingleField benchmark
	if *benchmark == BENCHMARK_SEARCH {
		if *indexesAmount > 1 {
			panic("search not supported on multiple indexes!")
		}
		name := fmt.Sprintf("search: %d terms", len(queries))
		log.Println("Starting full-text queries benchmark")
		Benchmark(*conc, duration, &histogramMutex, *engine, name, *outfile, *reportingPeriod, w, SearchBenchmark(queries, benchmarkQueryField, indexes[0], opts, *debugLevel))
		os.Exit(0)
	}

	// Auto-suggest benchmark
	if *benchmark == BENCHMARK_SUGGEST {
		panic("not yet implemented!")
	}

	// ingest random documents
	if *random > 0 {
		indexes[0].Drop()
		err := indexes[0].Create()
		if err != nil {
			panic(err)
		}

		N := 1000
		generateRandomDocuments(random, N, maxDocPerIndex, indexes)

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
								1000, runtimeCPUs, 250, nil, *maxDocPerIndex)

						}
					} else if *dataset == REDDIT_DATASET {
						wr := &ingest.RedditReader{}
						if *fileName != "" {
							if err := ingest.ReadFile(*fileName, wr, idx, nil, redisearch.IndexingOptions{}, 1000, *maxDocPerIndex); err != nil {
								panic(err)
							}
						} else if *dirName != "" {
							ingest.ReadDir(*dirName, *fileMatch, wr, idx, nil, redisearch.IndexingOptions{},
								1000, runtimeCPUs, 250, nil, *maxDocPerIndex)
						}
					} else if *dataset == PMC_DATASET {
						wr := &ingest.PmcReader{}
						if *fileName != "" {
							if err := ingest.ReadFile(*fileName, wr, idx, nil, redisearch.IndexingOptions{}, 1, *maxDocPerIndex); err != nil {
								panic(err)
							}
						} else if *dirName != "" {
							ingest.ReadDir(*dirName, *fileMatch, wr, idx, nil, redisearch.IndexingOptions{},
								1000, runtimeCPUs, 250, nil, *maxDocPerIndex)
						}
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

func generateRandomDocuments(random *int, N int, maxDocPerIndex *int, indexes []index.Index) {
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
