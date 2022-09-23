[![license](https://img.shields.io/github/license/RediSearch/RediSearchBenchmark.svg)](https://github.com/RediSearch/RediSearchBenchmark)
[![Forum](https://img.shields.io/badge/Forum-RediSearch-blue)](https://forum.redislabs.com/c/modules/redisearch/)
[![GoDoc](https://godoc.org/github.com/RediSearch/RediSearchBenchmark?status.svg)](https://godoc.org/github.com/RediSearch/RediSearchBenchmark)

# document-benchmark
This is a Go application (originally written by Dvir Volk) which supports reading, indexing and searching using two search engines:

* [RediSearch](https://github.com/RediSearch/RediSearch)
* [Elasticsearch](https://www.elastic.co/)

with the following datasets:

* [Wikipedia Abstract Data Dumps](https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/enwiki-abstract/enwiki-latest-abstract.xml): from English-language Wikipedia:Database page abstracts. This use case generates 3 TEXT fields per document.
* [pmc](https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/pmc/documents.json.bz2): Full text benchmark with academic papers from PMC.



## Getting Started

### Download Standalone binaries ( no Golang needed )

If you don't have go on your machine and just want to use the produced binaries you can download the following prebuilt bins:

https://github.com/RediSearch/RediSearchBenchmark/releases/latest

Here's how:

**Linux**

x86
```
wget -c https://github.com/RediSearch/RediSearchBenchmark/releases/latest/download/document-benchmark-linux-amd64.tar.gz -O - | tar -xz

# give it a try
./document-benchmark --help
```

arm64
```
wget -c https://github.com/RediSearch/RediSearchBenchmark/releases/latest/download/document-benchmark-linux-arm64.tar.gz -O - | tar -xz

# give it a try
./document-benchmark --help
```

**OSX**

x86
```
wget -c https://github.com/RediSearch/RediSearchBenchmark/releases/latest/download/document-benchmark-darwin-amd64.tar.gz -O - | tar -xz

# give it a try
./document-benchmark --help
```

arm64
```
wget -c https://github.com/RediSearch/RediSearchBenchmark/releases/latest/download/document-benchmark-darwin-arm64.tar.gz -O - | tar -xz

# give it a try
./document-benchmark --help
```

**Windows**
```
wget -c https://github.com/RediSearch/RediSearchBenchmark/releases/latest/download/document-benchmark-windows-amd64.tar.gz -O - | tar -xz

# give it a try
./document-benchmark --help
```

### Installation in a Golang env

The easiest way to get and install the benchmark utility with a Go Env is to use
`go get` and then `go install`:
```bash
# Fetch this repo
go get github.com/RediSearch/RediSearchBenchmark
cd $GOPATH/src/github.com/RediSearch/RediSearchBenchmark
make
```

## Example:

```

### Benchmarking RediSearch

* Populate Data:
```
./document-benchmark -hosts "host:port" -engine redis -file enwiki-latest-abstract.xml
```

* Run the benchmark:
```
./document-benchmark -hosts "host:port" -engine redis -benchmark search -queries -c 32 
```


## Usage 

```
$ ./bin/document-benchmark --help
Usage of ./bin/document-benchmark:
-benchmark string
The benchmark to run. One of: [search|prefix|wildcard]. If empty will not run.
-benchmark-query-fieldname string
fieldname to use for search|prefix|wildcard benchmarks. If empty will use the default per dataset.
-c int
benchmark concurrency (default 8)
-dataset string
The dataset tp process. One of: [enwiki|reddit|pmc] (default "enwiki")
-debug-level int
print debug info according to debug level. If 0 disabled.
-dir string
Recursively read all files in a directory
-distinct-terms int
When reading terms from input files how many terms should be read. (default 100000)
-duration int
number of seconds to run the benchmark (default 60)
-engine string
The search backend to run. One of: [redis|elastic|solr] (default "redis")
-es.number_of_replicas int
elastic replica count
-es.number_of_shards int
elastic shard count (default 1)
-es.requests.cache.enable
for elastic only. enable query cache. (default true)
-file string
Input file to ingest data from (wikipedia abstracts)
-hosts string
comma separated list of host:port to redis nodes (default "localhost:6379")
-indexes int
number of indexes to generate (default 1)
-match string
When reading directories, match only files with this glob (default ".*")
-maxdocs int
specify the number of max docs per index, -1 for no limit (default -1)
-o string
results output file. set to - for stdout (default "benchmark.json")
-password string
database password
-queries -queries='barack obama'
comma separated list of queries to benchmark. Use this option only for the historical reasons via -queries='barack obama'. If you don't specify a value it will read the input file and randomize the input search terms
-random int
Generate random documents with terms like term0..term{N}
-redis.cmd.prefix string
Command prefix for FT module (default "FT")
-reporting-period duration
Period to report runtime stats (default 1s)
-seed int
PRNG seed. (default 12345)
-stopwords string
filtered stopwords for term creation (default "a,an,and,are,as,at,be,but,by,for,if,in,into,is,it,no,not,of,on,or,such,that,the,their,then,there,these,they,this,to,was,will,with")
-temporary int
for redisearch only, create a temporary index that will expire after the given amount of seconds, -1 mean no temporary (default -1)
-term-query-prefix-max-len int
Maximum prefix length for the generated term queries. (default 3)
-term-query-prefix-min-len int
Minimum prefix length for the generated term queries. (default 3)
-terms-property string
When we read the terms from the input file we read the text from the property specified in this option. If empty the default property field will be used. Default on 'enwiki' dataset = 'body'. Default on 'reddit' dataset = 'body' (default "body")
-user string
database username. If empty will use the default for each of the databases
-verbatim
for redisearch only. does not try to use stemming for query expansion but searches the query terms verbatim.
```