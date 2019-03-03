package elastic

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/RediSearch/RediSearchBenchmark/index"
	"github.com/RediSearch/RediSearchBenchmark/query"
	"gopkg.in/olivere/elastic.v6"
	"context"
)

// Index is an ElasticSearch index
type Index struct {
	conn *elastic.Client

	md   *index.Metadata
	name string
	typ  string
	disableCache bool
}

var conn *elastic.Client = nil

// NewIndex creates a new elasticSearch index with the given address and name. typ is the entity type
func NewIndex(addr, name, typ string, disableCache bool, md *index.Metadata) (*Index, error) {
	var err error
	if conn == nil{
		client := &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 200,
			},
			Timeout: 20000 * time.Millisecond,
		}
		conn, err = elastic.NewClient(elastic.SetURL(addr), elastic.SetHttpClient(client))
		if err != nil {
			return nil, err
		}
	}
	
	ret := &Index{
		conn: conn,
		md:   md,
		name: name,
		typ:  typ,
		disableCache: disableCache,
	}

	return ret, nil

}

type mappingProperty map[string]interface{}

type mapping struct {
	Properties map[string]mappingProperty `json:"properties"`
}

// convert a fieldType to elastic mapping type string
func fieldTypeString(f index.FieldType) (string, error) {
	switch f {
	case index.TextField:
		return "text", nil
	case index.NumericField:
		return "double", nil
	default:
		return "", errors.New("Unsupported field type")
	}
}

func (i *Index) GetName() string {
	return i.name
}

// Create creates the index and posts a mapping corresponding to our Metadata
func (i *Index) Create() error {

	doc := mapping{Properties: map[string]mappingProperty{}}
	for _, f := range i.md.Fields {
		doc.Properties[f.Name] = mappingProperty{}
		fs, err := fieldTypeString(f.Type)
		if err != nil {
			return err
		}
		doc.Properties[f.Name]["type"] = fs
	}

	// we currently manually create the autocomplete mapping
	// ac := mapping{
	// 	Properties: map[string]mappingProperty{
	// 		"sugg": mappingProperty{
	// 			"type":     "completion",
	// 			"payloads": true,
	// 		},
	// 	},
	// }	

	mappings := map[string]mapping{
		i.typ:          doc,
		// "autocomplete": ac,
	}

	settings := map[string]interface{}{
		"index.requests.cache.enable": !i.disableCache,
		// "autocomplete": ac,
	}

	_, err := i.conn.CreateIndex(i.name).BodyJson(map[string]interface{}{"mappings": mappings, "settings": settings}).Do(context.Background())

	if err != nil {
		panic(err)
	}

	return err
}

// Index indexes multiple documents
func (i *Index) Index(docs []index.Document, opts interface{}) error {

	blk := i.conn.Bulk()
	for _, doc := range docs {
		req := elastic.NewBulkIndexRequest().Index(i.name).Type("doc").Id(doc.Id).Doc(doc.Properties)
		blk.Add(req)

	}
	_, err := blk.Refresh("true").Do(context.Background())

	if err != nil{
		panic(err)
	}

	return err
}

// Search searches the index for the given query, and returns documents,
// the total number of results, or an error if something went wrong
func (i *Index) Search(q query.Query) ([]index.Document, int, error) {

	eq := elastic.NewQueryStringQuery(q.Term)
	res, err := i.conn.Search(i.name).Type("doc").
		Query(eq).
		From(q.Paging.Offset).
		Size(q.Paging.Num).
		Do(context.Background())

	if err != nil {
		panic(err)
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

// Drop deletes the index
func (i *Index) Drop() error {
	i.conn.DeleteIndex(i.name).Do(context.Background())

	return nil
}

// AddTerms add suggestion terms to the suggester index
func (i *Index) AddTerms(terms ...index.Suggestion) error {
	blk := i.conn.Bulk()

	for _, term := range terms {
		req := elastic.NewBulkIndexRequest().Index(i.name).Type("autocomplete").
			Doc(map[string]interface{}{"sugg": term.Term})

		blk.Add(req)

	}
	_, err := blk.Refresh("true").Do(context.Background())

	return err

}

// Suggest gets completion suggestions for a given prefix.
// TODO: fuzzy not supported yet
func (i *Index) Suggest(prefix string, num int, fuzzy bool) ([]index.Suggestion, error) {

	// s := elastic.NewCompletionSuggester("autocomplete").Field("sugg").Text(prefix).Size(num)

	// res, err := i.conn.Suggest(i.name).Suggester(s).Do(context.Background())
	// if err != nil {
	// 	return nil, err
	// }

	// if suggs, found := res["autocomplete"]; found {
	// 	if len(suggs) > 0 {
	// 		opts := suggs[0].Options

	// 		ret := make([]index.Suggestion, 0, len(opts))
	// 		for _, op := range opts {
	// 			ret = append(ret, index.Suggestion{Term: op.Text, Score: float64(op.Score)})
	// 		}
	// 		return ret, nil
	// 	}

	// }

	//ret := make([]index.Suggestion, res.)
	return nil, nil

}

// Delete the suggestion index, currently just calls Drop()
func (i *Index) Delete() error {
	return i.Drop()
}
