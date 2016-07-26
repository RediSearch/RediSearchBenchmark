package solr

import (
	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
)

type Index struct{}

func (i *Index) Index(documents []index.Document, options interface{}) error {
	return nil
}
func (i *Index) Search(query.Query) (docs []index.Document, total int, err error) {
	return nil, 0, nil
}
func (i *Index) Drop() error {
	return nil
}
func (i *Index) Create() error {
	return nil
}
