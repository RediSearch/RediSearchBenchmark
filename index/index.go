package index

import (
	"github.com/RediSearch/RediSearchBenchmark/query"
)

// Index is the abstract representation of a search index we're working against.
// It is implemented for redisearch, elasticserch and solr.
type Index interface {
	GetName() string
	Index(documents []Document, options interface{}) error
	Search(query.Query) (docs []Document, total int, err error)
	PrefixSearch(query.Query) (docs []Document, total int, err error)
	Drop() error
	Create() error
}
