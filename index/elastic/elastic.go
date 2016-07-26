package elastic

import (
	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
	elastigo "github.com/mattbaird/elastigo/lib"
)

type Index struct {
	conn *elastigo.Conn
	bi   *elastigo.BulkIndexer
	md   *index.Metadata
	name string
}

func NewIndex(addr, name string, md *index.Metadata) *Index {

	ret := &Index{
		conn: elastigo.NewConn(),
		md:   md,
		name: name,
	}

	ret.conn.SetFromUrl(addr)

	return ret

}

func (i *Index) Create() error {

	_, err := i.conn.CreateIndex(i.name)

	return err
}

// Add indexes one entry in the index.
// TODO: Add support for multiple insertions
func (i *Index) Index(docs []index.Document, opts interface{}) error {

	if i.bi == nil {
		i.bi = i.conn.NewBulkIndexer(4)
		i.bi.Start()
	}

	for _, doc := range docs {

		if err := i.bi.Index(i.name, "doc", doc.Id, "", "", nil, doc.Properties); err != nil {
			return err
		}
	}
	i.bi.Flush()

	return nil
}

func (i *Index) Search(q query.Query) ([]index.Document, int, error) {

	resp, err := i.conn.SearchUri(i.name, "doc",
		map[string]interface{}{"q": q.Term}) //, "from": q.Paging.Offset, "size": q.Paging.Num})
	if err != nil {
		return nil, 0, err
	}
	_ = resp

	//	for _, h := range resp.Hits.Hits {
	//		fmt.Println(h.Fields)
	//	}

	//fmt.Println(out.Hits.Total)
	return nil, 0, err
	//out, err := i.conn..Search("testindex", "user", nil, searchJson)
}

func (i *Index) Drop() error {
	_, err := i.conn.DeleteIndex(i.name)
	if err != nil && err.Error() != "record not found" {
		return err
	}
	return nil
}
