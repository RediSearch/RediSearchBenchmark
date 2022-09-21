package redisearch

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"

	"time"

	"github.com/RediSearch/RediSearchBenchmark/index"
	"github.com/RediSearch/RediSearchBenchmark/query"
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

type ConnectionPool struct {
	sync.Mutex
	pools map[string]*redis.Pool
}

var connectionPool = ConnectionPool{
	pools: map[string]*redis.Pool{},
}

// Index is an interface to redisearch's redis connads
type Index struct {
	sync.Mutex
	hosts         []string
	password      string
	temporary     int
	md            *index.Metadata
	name          string
	commandPrefix string
}

var maxConns = 500

func (i *Index) getConn() redis.Conn {
	connectionPool.Lock()
	defer connectionPool.Unlock()
	host := i.hosts[rand.Intn(len(i.hosts))]
	pool, found := connectionPool.pools[host]
	if !found {
		pool = redis.NewPool(func() (redis.Conn, error) {
			// TODO: Add timeouts. and 2 separate pools for indexing and querying, with different timeouts
			if i.password != "" {
				return redis.Dial("tcp", host, redis.DialPassword(i.password))
			} else {
				return redis.Dial("tcp", host)
			}

		}, maxConns)
		pool.TestOnBorrow = func(c redis.Conn, t time.Time) error {
			if time.Since(t).Seconds() > 3 {
				_, err := c.Do("PING")
				return err
			}
			return nil
		}

		connectionPool.pools[host] = pool
	}
	return pool.Get()

}

// NewIndex creates a new index connecting to the redis host, and using the given name as key prefix
func NewIndex(addrs []string, pass string, temporary int, name string, md *index.Metadata) *Index {

	ret := &Index{

		hosts: addrs,

		md:        md,
		password:  pass,
		temporary: temporary,

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
	//ret.pool.MaxActive = ret.pool.MaxIdle

	return ret

}

func (i *Index) GetName() string {
	return i.name
}

// Create configues the index and creates it on redis
func (i *Index) Create() error {

	args := redis.Args{i.name}
	if i.temporary != -1 {
		t := strconv.Itoa(i.temporary)
		args = append(args, "TEMPORARY", t)
	}
	args = append(args, "SCHEMA")

	for _, f := range i.md.Fields {

		switch f.Type {
		case index.TextField:

			opts, ok := f.Options.(index.TextFieldOptions)
			if !ok {
				return errors.New("Invalid text field options type")
			}
			args = append(args, f.Name, "TEXT", "WEIGHT", opts.Weight)
			if opts.Sortable {
				args = append(args, "SORTABLE")
			}

			// stemming per field not supported yet

		case index.NumericField:
			args = append(args, f.Name, "NUMERIC")

		case index.NoIndexField:
			continue

		default:
			return fmt.Errorf("Unsupported field type %v", f.Type)
		}

	}

	conn := i.getConn()
	defer conn.Close()
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

	conn := i.getConn()
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

func (i *Index) PrefixQuery(q query.Query, verbose int) (docs []index.Document, total int, err error) {
	return i.FullTextQuerySingleField(q, verbose)
}

func (i *Index) WildCardQuery(q query.Query, verbose int) (docs []index.Document, total int, err error) {
	return i.FullTextQuerySingleField(q, verbose)
}

// Search searches the index for the given query, and returns documents,
// the total number of results, or an error if something went wrong
func (i *Index) FullTextQuerySingleField(q query.Query, verbose int) (docs []index.Document, total int, err error) {
	conn := i.getConn()
	defer conn.Close()
	term := q.Term
	if q.Flags&query.QueryTypePrefix != 0 && term[len(term)-1] != '*' {
		term = fmt.Sprintf("%s*", term)
	}
	queryParam := term
	if q.Field != "" {
		queryParam = fmt.Sprintf("@%s:%s", q.Field, term)
	}
	args := redis.Args{i.name, queryParam, "LIMIT", q.Paging.Offset, q.Paging.Num, "WITHSCORES"}
	//if q.Flags&query.QueryVerbatim != 0 {
	//	args = append(args, "VERBATIM")
	//}
	//if q.Flags&query.QueryNoContent != 0 {
	//	args = append(args, "NOCONTENT")
	//}

	if q.HighlightOpts != nil {
		args = args.Add("HIGHLIGHT")
		if q.HighlightOpts.Fields != nil && len(q.HighlightOpts.Fields) > 0 {
			args = args.Add("FIELDS", len(q.HighlightOpts.Fields))
			args = args.AddFlat(q.HighlightOpts.Fields)
		}
		args = args.Add("TAGS", q.HighlightOpts.Tags[0], q.HighlightOpts.Tags[1])
	}

	if q.SummarizeOpts != nil {
		args = args.Add("SUMMARIZE")
		if q.SummarizeOpts.Fields != nil && len(q.SummarizeOpts.Fields) > 0 {
			args = args.Add("FIELDS", len(q.SummarizeOpts.Fields))
			args = args.AddFlat(q.SummarizeOpts.Fields)
		}
		if q.SummarizeOpts.FragmentLen > 0 {
			args = args.Add("LEN", q.SummarizeOpts.FragmentLen)
		}
		if q.SummarizeOpts.NumFragments > 0 {
			args = args.Add("FRAGS", q.SummarizeOpts.NumFragments)
		}
		if q.SummarizeOpts.Separator != "" {
			args = args.Add("SEPARATOR", q.SummarizeOpts.Separator)
		}
	}

	if err := conn.Send(i.commandPrefix+".SEARCH", args...); err != nil {
		panic(err)
	}

	if err := conn.Flush(); err != nil {
		panic(err)
	}

	if _, err := conn.Receive(); err != nil {
		panic(err)
	}

	res, err := redis.Values(conn.Do(i.commandPrefix+".SEARCH", args...))
	if err != nil {
		return nil, 0, err
	}

	if total, err = redis.Int(res[0], nil); err != nil {
		return nil, 0, err
	}
	if verbose > 1 {
		log.Printf(
			"query %v. %d hits",
			args,
			total,
		)
	}
	return nil, total, nil
}

func (i *Index) Drop() error {
	conn := i.getConn()
	defer conn.Close()

	_, err := conn.Do("FT.DROP", i.name)
	return err

}
