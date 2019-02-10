[![license](https://img.shields.io/github/license/RediSearch/RediSearchBenchmark.svg)](https://github.com/RediSearch/RediSearchBenchmark)
<!--[![CircleCI](https://circleci.com/gh/RediSearch/RediSearchBenchmark/tree/master.svg?style=svg)](https://circleci.com/gh/RediSearch/RediSearchBenchmark/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RediSearch/RediSearchBenchmark.svg)](https://github.com/RediSearch/RediSearchBenchmark/releases/latest)
[![Codecov](https://codecov.io/gh/RediSearch/RediSearchBenchmark/branch/master/graph/badge.svg)](https://codecov.io/gh/RediSearch/RediSearchBenchmark) -->
[![GoDoc](https://godoc.org/github.com/RediSearch/RediSearchBenchmark?status.svg)](https://godoc.org/github.com/RediSearch/RediSearchBenchmark)


# RediSearchBenchmarks

Source code for benchmarking the RediSearch module, providing scalable high performance full-text search.

## What's in here

This is a Go application that can ingest data into the search engine, and benchmark the throughput and latency of running these benchmarks.

It supports reading [Wikipedia Abstract Data Dumps](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml) and indexing them, in three search engines: 

* [RediSearch](https://github.com/RedisLabsModules/RediSearch)
* [ElasticSearch](https://www.elastic.co/)
* [Solr](http://lucene.apache.org/solr/)

## Some Results

Output of the benchmark, running on a full Abstracts dump of the English wikipedia, on 5 redis shards over a `c4.4xlarge` EC2 instance:

Benchmark | Concurrent Clients | Throughput (requests/sec) | Average Latency (ms)
--- | --- | --- | --- 
search: hello | 1 | 2737.57 | 0.36
search: hello | 8 | 9706.18 | 0.80
search: hello | 16 | 11201.99 | 1.39
search: hello|32|13198.80|2.36
search: hello|64|17663.32|3.51
|  |  | | 
search: barack obama|64|16482.96|3.77
search: barack obama|1|2505.68|0.40
search: barack obama|8|8522.50|0.91
search: barack obama|16|10346.83|1.50
search: barack obama|32|11720.58|2.66
| | | | 
exact: "united states of america"|1|618.86|1.62
exact: "united states of america"|8|816.22|9.38
exact: "united states of america"|16|816.47|18.97
exact: "united states of america"|32|815.41|37.44
exact: "united states of america"|64|801.75|75.61
| | | |
search: manchester united|1|1513.97|0.66
search: manchester united|4|3192.11|1.23
search: manchester united|16|3485.66|4.43
search: manchester united|32|3512.08|8.80
search: manchester united|64|3559.03|17.29
| | | | 
autocomplete (2-3 letters) |1|4145.90|0.24
autocomplete (2-3 letters) |4|9691.04|0.41
autocomplete (2-3 letters) |8|12129.34|0.64
autocomplete (2-3 letters) |16|15268.47|1.00
autocomplete (2-3 letters) |32|16064.66|1.90
autocomplete (2-3 letters) |64|17255.77|3.51
autocomplete (2-3 letters) |128|17935.49|6.47


## Benchmark output

For each benchmark, we append a single line to a CSV file, with the engine used, benchmark type, query, concurrency, throughput and latency.

The default file name is `benchmark.csv`, and running the app with `-o -` will result in the result printed to stdout.

The output for running a benchmark on the queries "foo,bar,baz" with 4 concurrent clients, looks like this:

```
redis,"search: foo,bar,baz",4,14997.81,0.27
``` 

## Usage

```
Usage of ./RediSearchBenchmark:
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
  -shards int
    	the number of partitions we want (AT LEAST the number of cluster shards) (default 1)
```

## Example: Indexing documents into RediSearch

```
./RediSearchBenchmark -engine redis -shards 4 -hosts "localhost:6379,localhost:6380,localhost:6381,localhost:6382" \
    -file ~/wiki/enwiki-20160305-abstract.xml -scores ~/wiki/scores.csv
```

## Example: Benchmarking RediSearch with 32 concurrent clients

```
./RediSearchBenchmark -engine redis -shards 4 -hosts "localhost:6379,localhost:6380,localhost:6381,localhost:6382" \
    -benchmark search -queries "hello world,foo bar" -c 32 -o out.csv
```

