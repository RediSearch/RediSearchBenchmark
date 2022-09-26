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

| OS | Arch | Link |
| :---         |     :---:      |          ---: |
| Windows   | amd64  (64-bit X86)     | [document-benchmark-windows-amd64.exe](https://github.com/RediSearch/RediSearchBenchmark/releases/latest/download/document-benchmark-windows-amd64.exe.tar.gz)    |
| Linux   | amd64  (64-bit X86)     | [document-benchmark-linux-amd64](https://github.com/RediSearch/RediSearchBenchmark/releases/latest/download/document-benchmark-linux-amd64.tar.gz)    |
| Linux   | arm64 (64-bit ARM)     | [document-benchmark-linux-arm64](https://github.com/RediSearch/RediSearchBenchmark/releases/latest/download/document-benchmark-linux-arm64.tar.gz)    |
| Darwin   | amd64  (64-bit X86)     | [document-benchmark-darwin-amd64](https://github.com/RediSearch/RediSearchBenchmark/releases/latest/download/document-benchmark-darwin-amd64.tar.gz)    |
| Darwin   | arm64 (64-bit ARM)     | [document-benchmark-darwin-arm64](https://github.com/RediSearch/RediSearchBenchmark/releases/latest/download/document-benchmark-darwin-arm64.tar.gz)    |

Here's how bash script to download and try it:

```bash
wget -c https://github.com/RediSearch/RediSearchBenchmark/releases/latest/download/document-benchmark-$(uname -mrs | awk '{ print tolower($1) }')-$(dpkg --print-architecture).tar.gz -O - | tar -xz

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


## Try it out

To try it out locally we can use docker in the following manner to spin up both a Redis and Elastic environments:
```
sudo sysctl -w vm.max_map_count=262144
docker run -d -p 9200:9200 -p 9300:9300 -e "ELASTIC_PASSWORD=password"  docker.elastic.co/elasticsearch/elasticsearch:8.3.3
docker run -d -p 6379:6379 redis/redis-stack:edge
``` 

* Retrieve the wikipedia dataset, and populate with 1000000 documents:

```
wget https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/enwiki-abstract/enwiki-latest-abstract.xml
```

* Populate into RediSearch:
```
./bin/document-benchmark -hosts "127.0.0.1:6379" -engine redis -file enwiki-latest-abstract.xml -maxdocs 100000
```

* Populate into ElasticSearch:
```
./bin/document-benchmark -hosts "https://127.0.0.1:9200" -engine elastic -password "password" -file enwiki-latest-abstract.xml -maxdocs 100000
```

* Run the RediSearch benchmark:
```
./bin/document-benchmark -hosts "127.0.0.1:6379" -engine redis -benchmark search -file enwiki-latest-abstract.xml
```

* Run the ElasticSearch benchmark:
```
./bin/document-benchmark -hosts "https://127.0.0.1:9200" -engine elastic -password "password" -file enwiki-latest-abstract.xml -benchmark search 
```
