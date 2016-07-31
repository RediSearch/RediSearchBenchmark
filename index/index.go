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

type Suggestion struct {
	Term  string
	Score float64
}
type Autocompleter interface {
	AddTerms(terms ...Suggestion) error
	Suggest(prefix string, num int, fuzzy bool) ([]Suggestion, error)
	Delete() error
}
