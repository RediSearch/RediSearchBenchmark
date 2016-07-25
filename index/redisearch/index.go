package redisearch

import (
	"errors"
	"fmt"
	"time"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/RedisLabs/RediSearchBenchmark/query"
	"github.com/garyburd/redigo/redis"
)

type IndexingOptions struct {
	Language string
	Stemming bool
	NoSave   bool
}

type Index struct {
	pool *redis.Pool
	md   *index.Metadata
	name string
}

const (
	QueryTimeout = 100 * time.Millisecond
	IndexTimeout = time.Second
)

var MaxConns = 100

func NewIndex(addr, name string, md *index.Metadata) *Index {

	ret := &Index{
		pool: redis.NewPool(func() (redis.Conn, error) {
			return redis.Dial("tcp", addr)
		}, MaxConns),
		md:   md,
		name: name,
	}
	ret.pool.TestOnBorrow = nil
	ret.pool.MaxActive = ret.pool.MaxIdle

	return ret

}

func (i *Index) Create() error {

	args := redis.Args{i.name}

	for _, f := range i.md.Fields {
		switch f.Type {
		case index.TextField:

			opts, ok := f.Options.(index.TextFieldOptions)
			if !ok {
				return errors.New("Invalid text field options type")
			}
			args = append(args, f.Name, opts.Weight)
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
	_, err := conn.Do("FT.CREATE", args...)
	return err
}

// Add indexes one entry in the index.
// TODO: Add support for multiple insertions
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

		if err := conn.Send("FT.ADD", args...); err != nil {
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

func loadDocument(id, fields interface{}) (index.Document, error) {

	doc := index.NewDocument(string(id.([]byte)), 1.0)
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

func (i *Index) Search(q query.Query) (docs []index.Document, total int, err error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{i.name, q.Term, "LIMIT", q.Paging.Offset, q.Paging.Num}
	if q.Flags&query.QueryVerbatim != 0 {
		args = append(args, "VERBATIM")
	}
	if q.Flags&query.QueryNoContent != 0 {
		args = append(args, "NOCONTENT")
	}

	res, err := redis.Values(conn.Do("FT.SEARCH", args...))
	if err != nil {
		return
	}

	if total, err = redis.Int(res[0], nil); err != nil {
		return
	}

	docs = make([]index.Document, 0, len(res)-1)

	if len(res) > 2 {
		for i := 1; i < len(res); i += 2 {

			if i == 0 {
				continue
			}
			if d, e := loadDocument(res[i], res[i+1]); e == nil {
				docs = append(docs, d)
			}
		}
	}
	return
}

func (i *Index) Drop() error {
	conn := i.pool.Get()
	defer conn.Close()

	_, err := conn.Do("FLUSHDB")
	return err

}
