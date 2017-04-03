package redisearch

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
	"github.com/garyburd/redigo/redis"
)

// IndexingOptions are flags passed to the the abstract Index call, which receives them as interface{}, allowing
// for implementation specific options
type IndexingOptions struct {
	// the language of the document, for stemmer analysis
	Language string
	// whether we should use stemming on the document. NOT SUPPORTED BY THE ENGINE YET!
	Stemming bool

	// If set, we will not save the documents contents, just index them, for fetching ids only
	NoSave bool

	NoFieldFlags bool

	NoScoreIndexes bool

	NoOffsetVectors bool

	Prefix string
}

// Index is an interface to redisearch's redis connads
type Index struct {
	pool *redis.Pool

	md            *index.Metadata
	name          string
	commandPrefix string
}

var maxConns = 500

// NewIndex creates a new index connecting to the redis host, and using the given name as key prefix
func NewIndex(addr, name string, md *index.Metadata) *Index {

	ret := &Index{

		pool: redis.NewPool(func() (redis.Conn, error) {
			// TODO: Add timeouts. and 2 separate pools for indexing and querying, with different timeouts
			return redis.Dial("tcp", addr)
		}, maxConns),
		md: md,

		name: name,

		commandPrefix: "FT",
	}
	if md != nil && md.Options != nil {
		if opts, ok := md.Options.(IndexingOptions); ok {
			if opts.Prefix != "" {
				ret.commandPrefix = md.Options.(IndexingOptions).Prefix
			}
		}
	}
	ret.pool.TestOnBorrow = nil
	//ret.pool.MaxActive = ret.pool.MaxIdle

	return ret

}

// Create configues the index and creates it on redis
func (i *Index) Create() error {

	args := redis.Args{i.name, "SCHEMA"}

	for _, f := range i.md.Fields {

		switch f.Type {
		case index.TextField:

			opts, ok := f.Options.(index.TextFieldOptions)
			if !ok {
				return errors.New("Invalid text field options type")
			}
			args = append(args, f.Name, "TEXT", "WEIGHT", opts.Weight)

			// stemming per field not supported yet

		case index.NumericField:
			args = append(args, f.Name, "NUMERIC")

		case index.NoIndexField:
			continue

		default:
			return fmt.Errorf("Unsupported field type %v", f.Type)
		}

	}

	conn := i.pool.Get()
	defer conn.Close()
	fmt.Println(args)
	_, err := conn.Do(i.commandPrefix+".CREATE", args...)
	return err
}

// Index indexes multiple documents on the index, with optional IndexingOptions passed to options
func (i *Index) Index(docs []index.Document, options interface{}) error {

	var opts IndexingOptions
	hasOpts := false
	if options != nil {
		if opts, hasOpts = options.(IndexingOptions); !hasOpts {
			return errors.New("invalid indexing options")
		}
	}

	conn := i.pool.Get()
	defer conn.Close()

	n := 0

	for _, doc := range docs {
		args := make(redis.Args, 0, len(i.md.Fields)*2+4)
		args = append(args, i.name, doc.Id, doc.Score)
		// apply options
		if hasOpts {
			if opts.NoSave {
				args = append(args, "NOSAVE")
			}
			if opts.Language != "" {
				args = append(args, "LANGUAGE", opts.Language)
			}
		}

		args = append(args, "FIELDS")

		for k, f := range doc.Properties {
			args = append(args, k, f)
		}

		if err := conn.Send(i.commandPrefix+".ADD", args...); err != nil {
			return err
		}
		n++
	}

	if err := conn.Flush(); err != nil {
		return err
	}

	for n > 0 {
		if _, err := conn.Receive(); err != nil {
			return err
		}
		n--
	}

	return nil
}

// convert the result from a redis query to a proper Document object
func loadDocument(id, sc, fields interface{}) (index.Document, error) {

	score, err := strconv.ParseFloat(string(sc.([]byte)), 64)
	if err != nil {
		return index.Document{}, fmt.Errorf("Could not parse score: %s", err)
	}

	doc := index.NewDocument(string(id.([]byte)), float32(score))
	lst := fields.([]interface{})
	for i := 0; i < len(lst); i += 2 {
		prop := string(lst[i].([]byte))
		var val interface{}
		switch v := lst[i+1].(type) {
		case []byte:
			val = string(v)
		default:
			val = v

		}
		doc = doc.Set(prop, val)
	}
	return doc, nil
}

// Search searches the index for the given query, and returns documents,
// the total number of results, or an error if something went wrong
func (i *Index) Search(q query.Query) (docs []index.Document, total int, err error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{i.name, q.Term, "LIMIT", q.Paging.Offset, q.Paging.Num, "WITHSCORES"}
	//if q.Flags&query.QueryVerbatim != 0 {
	args = append(args, "VERBATIM")
	//}
	if q.Flags&query.QueryNoContent != 0 {
		args = append(args, "NOCONTENT")
	}

	res, err := redis.Values(conn.Do(i.commandPrefix+".SEARCH", args...))
	if err != nil {
		return
	}

	if total, err = redis.Int(res[0], nil); err != nil {
		return
	}

	docs = make([]index.Document, 0, len(res)-1)

	if len(res) > 3 {
		for i := 1; i < len(res); i += 3 {

			if i == 0 {
				continue
			}
			if d, e := loadDocument(res[i], res[i+1], res[i+2]); e == nil {
				docs = append(docs, d)
			}
		}
	}
	return
}

// Drop the index. Currentl just flushes the DB - note that this will delete EVERYTHING on the redis instance
func (i *Index) Drop() error {
	conn := i.pool.Get()
	defer conn.Close()

	_, err := conn.Do("FLUSHDB")
	return err

}
