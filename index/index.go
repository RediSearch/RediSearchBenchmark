package index

import (
	"github.com/RediSearch/RediSearchBenchmark/query"
)

// Index is the abstract representation of a search index we're working against.
// It is implemented for redisearch, elasticserch and solr.
type Index interface {
	GetName() string
	Index(documents []Document, options interface{}) error
	FullTextQuerySingleField(query.Query, int) (docs []Document, total int, err error)
	PrefixQuery(query.Query, int) (docs []Document, total int, err error)
	SuffixQuery(query.Query, int) (docs []Document, total int, err error)
	WildCardQuery(query.Query, int) (docs []Document, total int, err error)
	ContainsQuery(q query.Query, debug int) (docs []Document, total int, err error)
	Drop() error
	DocumentCount() int64
	Create() error
}
