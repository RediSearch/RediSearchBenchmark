package index

import (
	"github.com/RedisLabs/RediSearchBenchmark/query"
)

// Index is the abstract representation of a search index we're working against.
// It is implemented for redisearch, elasticserch and solr.
type Index interface {
	Index(documents []Document, options interface{}) error
	Search(query.Query) (docs []Document, total int, err error)
	Drop() error
	Create() error
}
