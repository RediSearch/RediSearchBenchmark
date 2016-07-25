package index

import (
	"github.com/RedisLabs/RediSearchBenchmark/query"
)

type Index interface {
	Index(documents []Document, options interface{}) error
	Search(query.Query) (docs []Document, total int, err error)
	Drop() error
	Create() error
}
