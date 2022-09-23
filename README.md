[![license](https://img.shields.io/github/license/RediSearch/RediSearchBenchmark.svg)](https://github.com/RediSearch/RediSearchBenchmark)
[![Forum](https://img.shields.io/badge/Forum-RediSearch-blue)](https://forum.redislabs.com/c/modules/redisearch/)
[![GoDoc](https://godoc.org/github.com/RediSearch/RediSearchBenchmark?status.svg)](https://godoc.org/github.com/RediSearch/RediSearchBenchmark)

# document-benchmarks
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

## Usage 

```
Usage of ./document-benchmark:
  -benchmark string
    	[search|suggest] - if set, we run the given benchmark
  -c int
    	benchmark concurrency (default 4)
  -duration int
    	number of seconds to run the benchmark (default 5)
  -engine string
        [redis|elastic|solr] The search backend to run (default "redis")
  -file string
    	Input file to ingest data from (wikipedia abstracts)
  -fuzzy
    	For redis only - benchmark fuzzy auto suggest
  -hosts string
    	comma separated list of host:port to redis nodes (default "localhost:6379")
  -o string
    	results output file. set to - for stdout (default "benchmark.csv")
  -queries string
    	comma separated list of queries to benchmark (default "hello world")
  -scores string
    	read scores of documents CSV for indexing
  -indexes int
      the total number of indexes to generate
  -disableCache
      for elastic only, disabling query cache
  -temporary int
      for redisearch only, create a temporary index that will expire after the given amount of seconds
  -maxdocs int
      specify the max numebr of docs per index, -1 for no limit
  -password string
      redis database password
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
