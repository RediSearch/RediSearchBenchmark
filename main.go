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
	"github.com/RediSearch/RediSearchBenchmark/ingest"
	"github.com/RediSearch/RediSearchBenchmark/query"
)

const (
	// IndexName is the name of our index on all engines
	IndexNamePrefix           = "rd"
	EN_WIKI_DATASET           = "enwiki"
	PMC_DATASET               = "pmc"
	REDDIT_DATASET            = "reddit"
	DEFAULT_DATASET           = EN_WIKI_DATASET
	BENCHMARK_SEARCH          = "search"
	BENCHMARK_PREFIX          = "prefix"
	BENCHMARK_CONTAINS        = "contains"
	BENCHMARK_SUFFIX          = "suffix"
	BENCHMARK_WILDCARD        = "wildcard"
	BENCHMARK_DEFAULT         = BENCHMARK_SEARCH
	ENGINE_REDIS              = "redis"
	ENGINE_ELASTIC            = "elastic"
	TERM_QUERY_MAX_LEN        = "term-query-prefix-max-len"
	ENGINE_DEFAULT            = ENGINE_REDIS
	DEFAULT_STOPWORDS         = "a,an,and,are,as,at,be,but,by,for,if,in,into,is,it,no,not,of,on,or,such,that,the,their,then,there,these,they,this,to,was,will,with"
	REDIS_MODE_SINGLE         = "single"
	REDIS_MODULE_OSS_CLUSTER  = "cluster"
	REDIS_MODE_SINGLE_DEFAULT = REDIS_MODE_SINGLE
)

// this mutex does not affect any of the client go-routines ( it's only to sync between main thread and datapoints processer go-routines )
var histogramMutex sync.Mutex

var indexMetadataEnWiki = index.NewMetadata().
	AddField(index.NewTextField("body", 1)).
	AddField(index.NewTextField("title", 1)).
	AddField(index.NewTextField("url", 1))

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
func selectIndex(indexMetadata *index.Metadata, engine string, hosts []string, user, pass string, temporary int, disableCache bool, name string, cmdPrefix string, shardCount, replicaCount, indexerNumCPUs int, tlsSkipVerify bool, bulkIndexerFlushIntervalSeconds int, bulkIndexerRefresh string, redisMode string, withSuffixTrie bool) (index.Index, interface{}) {

	switch engine {
	case ENGINE_REDIS:
		indexMetadata.Options = redisearch.IndexingOptions{Prefix: cmdPrefix}
		idx := redisearch.NewIndex(hosts, pass, temporary, name, indexMetadata, redisMode, withSuffixTrie)
		return idx, query.QueryVerbatim
	case ENGINE_ELASTIC:
		idx, err := elastic.NewIndex(hosts[0], name, "doc", disableCache, indexMetadata, user, pass, shardCount, replicaCount, indexerNumCPUs, tlsSkipVerify, bulkIndexerFlushIntervalSeconds, bulkIndexerRefresh)
		if err != nil {
			panic(err)
		}
		return idx, 0
	}
	panic("could not find index type " + engine)
}

func main() {
	runtimeCPUs := runtime.NumCPU()
	hosts := flag.String("hosts", "localhost:6379", "comma separated list of host:port to redis nodes")
	fileName := flag.String("file", "", "Input file to ingest data from (wikipedia abstracts)")
	engine := flag.String("engine", ENGINE_DEFAULT, fmt.Sprintf("The search backend to run. One of: [%s]", strings.Join([]string{ENGINE_REDIS, ENGINE_ELASTIC}, "|")))
	termsProperty := flag.String("terms-property", "body", "When we read the terms from the input file we read the text from the property specified in this option. If empty the default property field will be used. Default on 'enwiki' dataset = 'body'. Default on 'reddit' dataset = 'body'")
	termQueryPrefixMinLen := flag.Int64("term-query-prefix-min-len", 3, "Minimum prefix length for the generated term queries.")
	termQueryPrefixMaxLen := flag.Int64(TERM_QUERY_MAX_LEN, 3, "Maximum prefix length for the generated term queries.")
	totalTerms := flag.Int("distinct-terms", 100000, "When reading terms from input files how many terms should be read.")
	queryField := flag.String("benchmark-query-fieldname", "", "fieldname to use for search|prefix|wildcard benchmarks. If empty will use the default per dataset.")
	randomSeed := flag.Int64("seed", 12345, "PRNG seed.")
	termStopWords := flag.String("stopwords", DEFAULT_STOPWORDS, "filtered stopwords for term creation")
	dataset := flag.String("dataset", DEFAULT_DATASET, fmt.Sprintf("The dataset tp process. One of: [%s]", strings.Join([]string{EN_WIKI_DATASET, REDDIT_DATASET, PMC_DATASET}, "|")))
	benchmark := flag.String("benchmark", "", fmt.Sprintf("The benchmark to run. One of: [%s]. If empty will not run.", strings.Join([]string{BENCHMARK_SEARCH, BENCHMARK_PREFIX, BENCHMARK_WILDCARD, BENCHMARK_CONTAINS, BENCHMARK_SUFFIX}, "|")))

	tlsSkipVerify := flag.Bool("tls-skip-verify", true, "Skip verification of server certificate.")
	seconds := flag.Int("duration", 60, "number of seconds to run the benchmark")
	temporary := flag.Int("temporary", -1, "for redisearch only, create a temporary index that will expire after the given amount of seconds, -1 mean no temporary")
	conc := flag.Int("c", runtimeCPUs, "benchmark concurrency")
	debugLevel := flag.Int("debug-level", 0, "print debug info according to debug level. If 0 disabled.")
	maxDocPerIndex := flag.Int64("maxdocs", -1, "specify the number of max docs per index, -1 for no limit")
	outfile := flag.String("o", "benchmark.json", "results output file. set to - for stdout")

	password := flag.String("password", "", "database password")
	user := flag.String("user", "", "database username. If empty will use the default for each of the databases")
	reportingPeriod := flag.Duration("reporting-period", 1*time.Second, "Period to report runtime stats")
	bulkIndexingSizeDocs := flag.Int("bulk.indexer.ndocs", 100, "Groups the documents into chunks to index.")
	dropData := flag.Bool("drop-data-start", true, "Drop data at start.")

	// redis
	cmdPrefix := flag.String("redis.cmd.prefix", "FT", "Command prefix for FT module")
	redisMode := flag.String("redis.mode", REDIS_MODE_SINGLE_DEFAULT, fmt.Sprintf("Redis connection mode. One of: [%s]", strings.Join([]string{REDIS_MODE_SINGLE, REDIS_MODULE_OSS_CLUSTER}, "|")))
	verbatimEnabled := flag.Bool("redis.verbatim", false, "for redisearch only. does not try to use stemming for query expansion but searches the query terms verbatim.")
	withsuffixtrieEnabled := flag.Bool("redis.withsuffixtrie", false, "It is used to optimize contains (*foo*) and suffix (*foo) queries.")

	// elastic
	elasticShardCount := flag.Int("es.number_of_shards", 1, "elastic shard count")
	elasticReplicaCount := flag.Int("es.number_of_replicas", 0, "elastic replica count")
	elasticEnableCache := flag.Bool("es.requests.cache.enable", true, "for elastic only. enable query cache.")
	bulkIndexerRefresh := flag.String("es.refresh", "true", "If true, Elasticsearch refreshes the affected\n\t\t// shards to make this operation visible to search\n\t\t// if wait_for then wait for a refresh to make this operation visible to search,\n\t\t// if false do nothing with refreshes. Valid values: true, false, wait_for. Default: false.")
	bulkIndexerFlushIntervalSeconds := flag.Int("es.bulk.flush_interval_secs", 1, "ES bulk indexer flush interval.")

	nIdx := 1
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
	if *fileName == "" {
		fmt.Fprintln(os.Stderr, "No input file specified")
		flag.Usage()
		os.Exit(-1)
	}

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

	indexes := make([]index.Index, nIdx)
	var opts interface{}
	var queries []string
	var err error
	var indexMetadata *index.Metadata
	switch *dataset {
	case EN_WIKI_DATASET:
		indexMetadata = indexMetadataEnWiki
	case PMC_DATASET:
		indexMetadata = indexMetadataPMC
	}

	log.Printf("Using a total of %d concurrent benchmark workers", *conc)

	if *engine == "redis" && *verbatimEnabled {
		log.Println("Enabling VERBATIM mode on FullTextQuerySingleField benchmarks.")
		opts = query.QueryVerbatim
	}
	// select index to run
	name := IndexNamePrefix + strconv.Itoa(0)
	idx, _ := selectIndex(indexMetadata, *engine, servers, username, *password, *temporary, !*elasticEnableCache, name, *cmdPrefix, *elasticShardCount, *elasticReplicaCount, *conc, *tlsSkipVerify, *bulkIndexerFlushIntervalSeconds, *bulkIndexerRefresh, *redisMode, *withsuffixtrieEnabled)
	indexes[0] = idx

	if *benchmark != "" {
		w := new(tabwriter.Writer)
		w.Init(os.Stderr, 20, 0, 0, ' ', tabwriter.AlignRight)
		log.Println("Using input file to produce terms for the benchmarks")
		switch *dataset {
		case EN_WIKI_DATASET:
			wr := &ingest.WikipediaAbstractsReader{}
			if queries, err = ingest.ReadTerms(*fileName, wr, indexes[0], 0, 10000, *totalTerms, *termsProperty, strings.Split(*termStopWords, ",")); err != nil {
				log.Fatalf("Failed on Term preparation due to %v", err)
			}
		case PMC_DATASET:
			wr := &ingest.PmcReader{}
			if queries, err = ingest.ReadTerms(*fileName, wr, indexes[0], 0, 10000, *totalTerms, *termsProperty, strings.Split(*termStopWords, ",")); err != nil {
				log.Fatalf("Failed on Term preparation due to %v", err)
			}
		}
		returnCode := 0
		switch *benchmark {
		case BENCHMARK_CONTAINS:
			name := fmt.Sprintf("contains: %d terms", len(queries))
			log.Println("Starting term-level queries benchmark: Type CONTAINS")
			Benchmark(*conc, duration, &histogramMutex, *engine, name, *outfile, *reportingPeriod, w, ContainsBenchmark(queries, benchmarkQueryField, indexes[0], *termQueryPrefixMinLen, *termQueryPrefixMaxLen, *debugLevel))
		case BENCHMARK_WILDCARD:
			name := fmt.Sprintf("wildcard: %d terms", len(queries))
			log.Println("Starting term-level queries benchmark: Type WILDCARD")
			prefixMaxLen := *termQueryPrefixMaxLen
			if (prefixMaxLen - 2) <= *termQueryPrefixMinLen {
				prefixMaxLen = prefixMaxLen + 2
				log.Println(fmt.Sprintf("%s needs to be at least larger by 2 than min length given we want the wildcard to be present at the midle of the term. Forcing %s=%d", TERM_QUERY_MAX_LEN, TERM_QUERY_MAX_LEN, prefixMaxLen))
			}
			Benchmark(*conc, duration, &histogramMutex, *engine, name, *outfile, *reportingPeriod, w, WildcardBenchmark(queries, benchmarkQueryField, indexes[0], *termQueryPrefixMinLen, prefixMaxLen, *debugLevel))
		case BENCHMARK_SUFFIX:
			name := fmt.Sprintf("suffix: %d terms", len(queries))
			log.Println("Starting term-level queries benchmark: Type SUFFIX")
			Benchmark(*conc, duration, &histogramMutex, *engine, name, *outfile, *reportingPeriod, w, SuffixBenchmark(queries, benchmarkQueryField, indexes[0], *termQueryPrefixMinLen, *termQueryPrefixMaxLen, *debugLevel))
		case BENCHMARK_PREFIX:
			name := fmt.Sprintf("prefix: %d terms", len(queries))
			log.Println("Starting term-level queries benchmark: Type PREFIX")
			Benchmark(*conc, duration, &histogramMutex, *engine, name, *outfile, *reportingPeriod, w, PrefixBenchmark(queries, benchmarkQueryField, indexes[0], *termQueryPrefixMinLen, *termQueryPrefixMaxLen, *debugLevel))
		case BENCHMARK_SEARCH:
			name := fmt.Sprintf("search: %d terms", len(queries))
			log.Println("Starting full-text queries benchmark")
			Benchmark(*conc, duration, &histogramMutex, *engine, name, *outfile, *reportingPeriod, w, SearchBenchmark(queries, benchmarkQueryField, indexes[0], opts, *debugLevel))
		default:
			returnCode = -1
			fmt.Fprintln(os.Stderr, "No valid benchmark specified")
		}
		os.Exit(returnCode)

	} else {
		if *dropData {
			fmt.Println("Ensuring a clean DB at start of ingestion")
			err := idx.Drop()
			if err != nil {
				panic(err)
			}
			ndocs := idx.DocumentCount()
			if ndocs != 0 {
				fmt.Fprintln(os.Stderr, fmt.Sprintf("Expected %d documents in the index, but got %d.", 0, ndocs))
				os.Exit(-1)
			} else {
				log.Println(fmt.Sprintf("Confirmed that the index total documents is the expected value %d=%d", ndocs, 0))
			}
		}
		err := idx.Create()
		if err != nil {
			panic(err)
		}
		var reader ingest.DocumentReader

		switch *dataset {
		case EN_WIKI_DATASET:
			reader = &ingest.WikipediaAbstractsReader{}
		case REDDIT_DATASET:
			reader = &ingest.RedditReader{}
		case PMC_DATASET:
			reader = &ingest.PmcReader{}
		}
		err = ingest.ReadFile(*fileName, reader, idx, redisearch.IndexingOptions{}, *bulkIndexingSizeDocs, *maxDocPerIndex, *conc)

		if *maxDocPerIndex > 0 {
			ndocs := idx.DocumentCount()
			if ndocs != *maxDocPerIndex {
				fmt.Fprintln(os.Stderr, fmt.Sprintf("Expected %d documents in the index, but got %d.", *maxDocPerIndex, ndocs))
				os.Exit(-1)
			} else {
				log.Println(fmt.Sprintf("Confirmed that the index total documents is the expected value %d=%d", ndocs, *maxDocPerIndex))
			}
		}
		if err != nil {
			panic(err)
		}
		os.Exit(0)
	}

}
