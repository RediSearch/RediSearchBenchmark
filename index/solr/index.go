package solr

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"strings"

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
		// hack to support suggestions on the same index
		sd["suggest"] = strings.ToLower(doc.Properties["title"].(string))

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

func (i *Index) Close() {

}

func (i *Index) AddTerms(terms ...index.Suggestion) error {
	// not implemented since we do this automatically in the indexing itself
	return nil
}

// SuggestResponse parses the suggest responses because the solr client doesn't include this feature
type SuggestResponse struct {
	ResponseHeader struct {
		Status int `json:"status"`
		QTime  int `json:"QTime"`
	} `json:"responseHeader"`
	Suggest struct {
		Autocomplete map[string]struct {
			NumFound    int `json:"numFound"`
			Suggestions []struct {
				Term    string  `json:"term"`
				Weight  float64 `json:"weight"`
				Payload string  `json:"payload"`
			} `json:"suggestions"`
		} `json:"autocomplete"`
	} `json:"suggest"`
}

func (i *Index) Suggest(prefix string, num int, fuzzy bool) ([]index.Suggestion, error) {
	s := i.si.Search(solr.NewQuery())

	parms := url.Values{}
	parms.Set("suggest.q", prefix)
	parms.Set("suggest.num", fmt.Sprintf("%d", num))
	parms.Set("suggest", "true")
	b, err := s.Resource("suggest", &parms)
	if err != nil || b == nil {
		return nil, err
	}

	var res SuggestResponse

	if err := json.Unmarshal(*b, &res); err != nil {
		return nil, err
	}

	for _, s := range res.Suggest.Autocomplete {
		ret := make([]index.Suggestion, 0, num)
		for _, sugg := range s.Suggestions {
			ret = append(ret, index.Suggestion{sugg.Term, sugg.Weight})
		}

		return ret, nil
	}
	return nil, nil

}
func (i *Index) Delete() error {
	return nil
}
