package solr

import (
	"net/url"
	"reflect"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
	"github.com/vanng822/go-solr/solr"
)

type Index struct {
	si   *solr.SolrInterface
	name string
	md   *index.Metadata
}

func NewIndex(url, name string, md *index.Metadata) (*Index, error) {
	si, err := solr.NewSolrInterface(url, name)
	if err != nil {
		return nil, err
	}

	return &Index{
		si:   si,
		name: name,
		md:   md,
	}, nil

}

func (i *Index) Index(documents []index.Document, options interface{}) error {

	soldocs := make([]solr.Document, 0, len(documents))
	for _, doc := range documents {
		sd := solr.Document(doc.Properties)
		sd["id"] = doc.Id
		sd["score"] = doc.Score
		soldocs = append(soldocs, sd)
	}

	params := url.Values{"commit": []string{"true"}}
	_, err := i.si.Add(soldocs, len(soldocs), &params)
	return err
}
func (i *Index) Search(q query.Query) (docs []index.Document, total int, err error) {
	query := solr.NewQuery()
	query.Q(q.Term)
	query.AddParam("cache", "false")
	//query.Start(int(q.Paging.Offset))
	//query.Rows(int(q.Paging.Num))
	s := i.si.Search(query)
	r, err := s.Result(nil)
	if err != nil {
		return nil, 0, err
	}

	ret := make([]index.Document, 0, len(r.Results.Docs))
	for _, d := range r.Results.Docs {

		doc := index.NewDocument(d.Get("id").(string), 1.0)
		for k, v := range d {
			if reflect.TypeOf(v).Kind() == reflect.Slice {
				v = v.([]interface{})[0]
			}
			if k != "id" {
				doc.Set(k, v)
			}
		}
		ret = append(ret, doc)
	}

	return ret, r.Results.NumFound, nil
}
func (i *Index) Drop() error {

	_, err := i.si.DeleteAll()
	return err

}
func (i *Index) Create() error {

	ca, err := i.si.CoreAdmin()
	if err != nil {
		return err
	}

	params := url.Values{}
	params.Set("instanceDir", i.name)
	params.Set("name", i.name)
	_, err = ca.Action("CREATE", &params)
	return err
}
