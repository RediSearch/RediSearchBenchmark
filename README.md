# RediSearchBenchmarks

Source code for benchmarking the RediSearch module, providing scalable high performance full-text search.

## What's in here

This is a Go application that can ingest data into the search engine, and benchmark the throughput and latency of running these benchmarks.

It supports reading [Wikipedia Abstract Data Dumps](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml) and indexing them, in three search engines: 

* [RediSearch](https://github.com/RedisLabsModules/RediSearch)
* [ElasticSearch](https://www.elastic.co/)
* [Solr](http://lucene.apache.org/solr/)

## Benchmarking

### Benchmarking Redisearch
The following benchmark is running on a full Abstracts dump of the English wikipedia, on 5 shards over a `c4.8xlarge` EC2 instance, performing two-word search queries with 32 clients:

Steps:

* Create a 5 shard redisearch cluster on redis entrprise with the following configuration:
```
PARTITION AUTO MAXDOCTABLESIZE 10000000
```

* Populate Data:
```
```

* Run the benchmark:
```
```
Results:

Benchmark | Concurrent Clients | Throughput (requests/sec) | Average Latency (ms)
--- | --- | --- | --- 
two-word search | 32 | 12547 | 8

### Benchmarking Multi-tenant Redisearch
The following benchmark tests the amount of time it takes to create 50,000 indexes with 500 docs in each index, for a total of 25M documents. It uses the enterprise version of RediSearch on Redis Enterprise Cluster.

Steps:

* Create a 20 shard redisearch cluster on redis entrprise.

* Populate Data:
```
```
Results:

Indexing took: 3 minutes and 21 seconds


## Benchmark

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

