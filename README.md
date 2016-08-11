# RediSearchBenchmarks

Source code for benchmarking the RediSearch module, providing scalable high performance full-text search.

## What's in here

This is a Go application that can ingest data into the search engine, and benchmark the throughput and latency of running these benchmarks.

It supports reading [Wikipedia Abstract Data Dumps](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml) and indexing them, in three search engines: 

* [RediSearch](https://github.com/RedisLabsModules/RediSearch)
* [ElasticSearch](https://www.elastic.co/)
* [Solr](http://lucene.apache.org/solr/)

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

