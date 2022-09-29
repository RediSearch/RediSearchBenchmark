package elastic

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"context"
	"github.com/RediSearch/RediSearchBenchmark/index"
	"github.com/RediSearch/RediSearchBenchmark/query"
	"github.com/cenkalti/backoff/v4"
	elastic "github.com/elastic/go-elasticsearch/v8"
)

// Index is an ElasticSearch index
type Index struct {
	conn         *elastic.Client
	bi           esutil.BulkIndexer
	md           *index.Metadata
	name         string
	typ          string
	disableCache bool
	shardCount   int
	replicaCount int
}

// NewIndex creates a new elasticSearch index with the given address and name. typ is the entity type
func NewIndex(addr, name, typ string, disableCache bool, md *index.Metadata, user, pass string, shardCount, replicaCount, indexerNumCPUs int, tlsSkipVerify bool, bulkIndexerFlushIntervalSeconds int, bulkIndexerRefresh string) (*Index, error) {
	var err error
	retryBackoff := backoff.NewExponentialBackOff()
	elasticMaxRetriesPropDefault := 10
	cfg := elastic.Config{
		Addresses: strings.Split(addr, ","),
		// Retry on 429 TooManyRequests statuses
		RetryOnStatus: []int{502, 503, 504, 429},

		// Configure the backoff function
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},
		MaxRetries: elasticMaxRetriesPropDefault,
		Username:   user,
		Password:   pass,
		// Transport / SSL
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: tlsSkipVerify,
			},
		},
	}
	es, err := elastic.NewClient(cfg)
	if err != nil {
		fmt.Println(fmt.Sprintf("Error creating the elastic client: %v", err))
		return nil, err
	}
	fmt.Println("Connected to elastic!")
	// 1. Get cluster info
	var r map[string]interface{}
	res, err := es.Info()
	if err != nil {
		log.Fatalf("Error getting response: %v", err)
	}
	defer res.Body.Close()
	// Check response status
	if res.IsError() {
		log.Fatalf("Error: %s", res.String())
	}
	// Deserialize the response into a map.
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	// Print client and server version numbers.
	log.Printf("Elastic server info: %s", r["version"].(map[string]interface{})["number"])

	// Create the BulkIndexer
	var flushIntervalTime = bulkIndexerFlushIntervalSeconds * int(time.Second)
	bulkIndexerFlushBytes := int(5e+6)
	bulkIndexerNumCpus := indexerNumCPUs

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         name,                             // The default index name
		Client:        es,                               // The Elasticsearch client
		NumWorkers:    bulkIndexerNumCpus,               // The number of worker goroutines
		FlushBytes:    bulkIndexerFlushBytes,            // The flush threshold in bytes
		FlushInterval: time.Duration(flushIntervalTime), // The periodic flush interval
		// If true, Elasticsearch refreshes the affected
		// shards to make this operation visible to search
		// if wait_for then wait for a refresh to make this operation visible to search,
		// if false do nothing with refreshes. Valid values: true, false, wait_for. Default: false.
		Refresh: bulkIndexerRefresh,
	})
	if err != nil {
		fmt.Println("Error creating the elastic indexer: %v", err)
		return nil, err
	}

	ret := &Index{
		conn:         es,
		bi:           bi,
		md:           md,
		name:         name,
		typ:          typ,
		disableCache: disableCache,
		shardCount:   shardCount,
		replicaCount: replicaCount,
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

func (i *Index) DocumentCount() int64 {
	return 0
}

// Create creates the index and posts a mapping corresponding to our Metadata
func (i *Index) Create() error {
	mappings := mapping{Properties: map[string]mappingProperty{}}
	for _, f := range i.md.Fields {
		mappings.Properties[f.Name] = mappingProperty{}
		fs, err := fieldTypeString(f.Type)
		if err != nil {
			return err
		}
		mappings.Properties[f.Name]["type"] = fs
	}

	settings := map[string]interface{}{
		"index": map[string]interface{}{
			"number_of_shards":      i.shardCount,
			"number_of_replicas":    i.replicaCount,
			"requests.cache.enable": !i.disableCache,
		},
	}
	fmt.Println("Ensuring that if the index exists we recreat it")
	// Re-create the index
	var res *esapi.Response
	var err error
	if res, err = i.conn.Indices.Delete([]string{i.name}, i.conn.Indices.Delete.WithIgnoreUnavailable(true)); err != nil || res.IsError() {
		fmt.Println(fmt.Sprintf("Cannot delete index: %s", err))
		return err
	}
	res.Body.Close()
	// Define index mapping.
	mapping := map[string]interface{}{"mappings": mappings, "settings": settings}
	data, err := json.Marshal(mapping)
	if err != nil {
		fmt.Println(fmt.Sprintf("Cannot encode index mapping %v: %s", mapping, err))
		return err
	}
	res, err = i.conn.Indices.Create(i.name, i.conn.Indices.Create.WithBody(strings.NewReader(string(data))))
	if err != nil || res.IsError() {
		fmt.Println(fmt.Sprintf("Cannot create index: %v. %v", err, res.String()))
		return err
	}
	res.Body.Close()

	return err
}

// Index indexes multiple documents
func (i *Index) Index(docs []index.Document, opts interface{}) error {
	var err error
	for _, doc := range docs {
		data, err := json.Marshal(doc.Properties)
		if err != nil {
			return err
		}
		// Add an item to the BulkIndexer
		err = i.bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				// Action field configures the operation to perform (index, create, delete, update)
				Action: "index",

				// DocumentID is the (optional) document ID
				DocumentID: doc.Id,

				// Body is an `io.Reader` with the payload
				Body: bytes.NewReader(data),

				// OnSuccess is called for each successful operation
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				},
				// OnFailure is called for each failed operation
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						fmt.Printf("ERROR BULK INSERT: %s", err)
					} else {
						fmt.Printf("ERROR BULK INSERT: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
		if err != nil {
			fmt.Println("Unexpected error while bulk inserting: %s", err)
			return err
		}
	}
	return err
}

// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-prefix-query.html
func (i *Index) PrefixQuery(q query.Query, verbose int) ([]index.Document, int, error) {
	es := i.conn
	query := map[string]interface{}{
		"from": q.Paging.Offset,
		"size": q.Paging.Num,
		"query": map[string]interface{}{
			"prefix": map[string]interface{}{
				q.Field: map[string]interface{}{
					"value": q.Term,
				},
			},
		},
	}
	hits, err := elasticSearchQuery(i.name, es, verbose, query)
	return nil, hits, err
}

func elasticSearchQuery(indexName string, es *elastic.Client, verbose int, query map[string]interface{}) (int, error) {
	// Build the request body.
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}
	var r map[string]interface{}

	// Perform the search request.
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex(indexName),
		es.Search.WithBody(&buf),
		es.Search.WithTrackTotalHits(true))
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()
	hits := elasticParseResponse(r, verbose, res, query)
	return hits, err
}

func elasticParseResponse(r map[string]interface{}, verbose int, res *esapi.Response, query map[string]interface{}) int {
	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Fatalf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	// Print the response status, number of results, and request duration.
	hits := int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
	if verbose > 1 {
		log.Printf(
			"query %v. [%s] %d hits; took: %dms",
			query,
			res.Status(), hits,
			int(r["took"].(float64)),
		)
	}
	return hits
}

// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
func (i *Index) WildCardQuery(q query.Query, verbose int) ([]index.Document, int, error) {
	es := i.conn
	query := map[string]interface{}{
		"from": q.Paging.Offset,
		"size": q.Paging.Num,
		"query": map[string]interface{}{
			"wildcard": map[string]interface{}{
				q.Field: map[string]interface{}{
					"value": q.Term,
				},
			},
		},
	}
	hits, err := elasticSearchQuery(i.name, es, verbose, query)
	return nil, hits, err
}

// Search searches the index for the given query, and returns documents,
// the total number of results, or an error if something went wrong
// https://www.elastic.co/guide/en/elasticsearch/reference/current/full-text-queries.html
func (i *Index) FullTextQuerySingleField(q query.Query, verbose int) ([]index.Document, int, error) {

	query := map[string]interface{}{
		"from": q.Paging.Offset,
		"size": q.Paging.Num,
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				q.Field: q.Term,
			},
		},
	}

	es := i.conn

	hits, err := elasticSearchQuery(i.name, es, verbose, query)
	return nil, hits, err
}

// Drop deletes the index
func (i *Index) Drop() error {
	// Re-create the index
	var res *esapi.Response
	var err error
	if res, err = i.conn.Indices.Delete([]string{i.name}, i.conn.Indices.Delete.WithIgnoreUnavailable(true)); err != nil || res.IsError() {
		fmt.Println(fmt.Sprintf("Cannot delete index: %s", err))
		return err
	}
	res.Body.Close()
	return err
}

// Delete the suggestion index, currently just calls Drop()
func (i *Index) Delete() error {
	return i.Drop()
}
