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

type AutocompleteTerm struct {
	Term  string
	Score float64
}
type Autocompleter interface {
	AddTerms(terms ...AutocompleteTerm) error
	Suggest(prefix string, num int, fuzzy bool) ([]string, error)
	Delete() error
}
