[![license](https://img.shields.io/github/license/RediSearch/RediSearchBenchmark.svg)](https://github.com/RediSearch/RediSearchBenchmark)
<!--[![CircleCI](https://circleci.com/gh/RediSearch/RediSearchBenchmark/tree/master.svg?style=svg)](https://circleci.com/gh/RediSearch/RediSearchBenchmark/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RediSearch/RediSearchBenchmark.svg)](https://github.com/RediSearch/RediSearchBenchmark/releases/latest)
[![Codecov](https://codecov.io/gh/RediSearch/RediSearchBenchmark/branch/master/graph/badge.svg)](https://codecov.io/gh/RediSearch/RediSearchBenchmark) -->
[![GoDoc](https://godoc.org/github.com/RediSearch/RediSearchBenchmark?status.svg)](https://godoc.org/github.com/RediSearch/RediSearchBenchmark)

# RediSearchBenchmarks
This is a Go application (originally written by Dvir Volk) which supports reading, indexing and searching in [Wikipedia Abstract Data Dumps](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml) using two search engines:  

* [RediSearch](https://github.com/RedisLabsModules/RediSearch)
* [Elasticsearch](https://www.elastic.co/)

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

### Benchmarking RediSearch
The following commands perform a two words search query using 5 shards over a `c4.8xlarge` EC2 instance.

Steps:

* Create a 5 shard RediSearch cluster on Redis Enterprise with the following configuration:
```
PARTITION AUTO MAXDOCTABLESIZE 10000000
```

* Populate Data:
```
./RediSearchBenchmark -hosts "host:port" -engine redis -file enwiki-latest-abstract.xml
```

* Run the benchmark:
```
./RediSearchBenchmark -hosts "host:port" -engine redis -benchmark search -queries "Barack Obama" -c 32 
```


### Multi-Tenant RediSearch benchmark
The following benchmark tests the amount of time it takes to create 50,000 indexes with 500 docs in each index, for a total of 25M documents. 

Steps:

* Create a 20 shard RediSearch cluster on Redis Enterprise.

* Index Data:
```
./RediSearchBenchmark -hosts "host:port" -engine redis -file enwiki-latest-abstract.xml -indexes 50000 -maxdocs 500 -temporary 2147483647
```



