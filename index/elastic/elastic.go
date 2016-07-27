package elastic

import (
	"encoding/json"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
	"gopkg.in/olivere/elastic.v3"
)

type Index struct {
	conn *elastic.Client

	md   *index.Metadata
	name string
}

func NewIndex(addr, name string, md *index.Metadata) (*Index, error) {
	conn, err := elastic.NewClient(elastic.SetURL(addr))
	if err != nil {
		return nil, err
	}
	ret := &Index{
		conn: conn,
		md:   md,
		name: name,
	}

	return ret, nil

}

func (i *Index) Create() error {

	_, err := i.conn.CreateIndex(i.name).Do()

	return err
}

// Add indexes one entry in the index.
// TODO: Add support for multiple insertions
func (i *Index) Index(docs []index.Document, opts interface{}) error {

	blk := i.conn.Bulk()

	for _, doc := range docs {
		req := elastic.NewBulkIndexRequest().Index(i.name).Type("doc").Id(doc.Id).Doc(doc.Properties)
		blk.Add(req)

	}
	_, err := blk.Do()

	return err
}

func (i *Index) Search(q query.Query) ([]index.Document, int, error) {

	eq := elastic.NewQueryStringQuery(q.Term)
	res, err := i.conn.Search(i.name).Type("doc").
		Query(eq).
		From(q.Paging.Offset).
		Size(q.Paging.Num).
		Do()

	if err != nil {
		return nil, 0, err
	}

	ret := make([]index.Document, 0, q.Paging.Num)
	for _, h := range res.Hits.Hits {

		if h != nil {
			d := index.NewDocument(h.Id, float32(*h.Score))
			if err := json.Unmarshal(*h.Source, &d.Properties); err == nil {
				ret = append(ret, d)
			}
		}

	}

	return ret, int(res.TotalHits()), err
}

func (i *Index) Drop() error {
	i.conn.DeleteIndex(i.name).Do()
	//	elastic.
	//	if err != nil && !elastic.IsNotFound(err) {
	//		return err
	//	}

	return nil
}
