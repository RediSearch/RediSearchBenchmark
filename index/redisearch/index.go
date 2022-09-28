package redisearch

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/RediSearch/RediSearchBenchmark/index"
	"github.com/RediSearch/RediSearchBenchmark/query"
	goredis "github.com/go-redis/redis/v9"
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

var total int64 = 0
var mu sync.Mutex = sync.Mutex{}

type redisClient interface {
	Do(ctx context.Context, args ...interface{}) *goredis.Cmd
	FlushDB(ctx context.Context) *goredis.StatusCmd
	Close() error
}

// Index is an interface to redisearch's redis connads
type Index struct {
	sync.Mutex
	hosts            []string
	password         string
	temporary        int
	md               *index.Metadata
	name             string
	commandPrefix    string
	client           redisClient
	clientClient     *goredis.ClusterClient
	standaloneClient *goredis.Client
	cluster          bool
}

// NewIndex creates a new index connecting to the redis host, and using the given name as key prefix
func NewIndex(addrs []string, pass string, temporary int, name string, md *index.Metadata, mode string) *Index {
	ret := &Index{
		hosts: addrs,

		md:        md,
		password:  pass,
		temporary: temporary,

		name:          name,
		commandPrefix: "FT",
		cluster:       false,
	}
	switch mode {
	case "cluster":
		ret.cluster = true
		opts := &goredis.ClusterOptions{}
		opts.Addrs = addrs
		opts.MaxRedirects = 10
		opts.Password = pass
		clusterC := goredis.NewClusterClient(opts)
		ret.client = clusterC
		ret.clientClient = clusterC

	case "single":
		fallthrough
	default:
		opts := &goredis.Options{}
		opts.Network = "tcp"
		opts.Addr = addrs[0]
		opts.Password = pass
		standaloneC := goredis.NewClient(opts)
		ret.client = standaloneC
		ret.standaloneClient = standaloneC
	}

	if md != nil && md.Options != nil {
		if opts, ok := md.Options.(IndexingOptions); ok {
			if opts.Prefix != "" {
				ret.commandPrefix = md.Options.(IndexingOptions).Prefix
			}
		}
	}

	return ret

}

func (i *Index) DocumentCount() (count int64) {
	ctx := context.Background()
	if i.cluster {
		i.clientClient.ForEachMaster(ctx, docCountShard)
	} else {
		docCountShard(ctx, i.standaloneClient)
	}
	count = int64(total)
	return count
}

func docCountShard(ctx context.Context, conn *goredis.Client) (err error) {
	mu.Lock()
	defer mu.Unlock()
	res := strings.Split(conn.Do(ctx, "INFO", "KEYSPACE").String(), "\n")
	count := int64(0)
	if len(res) > 2 {
		db0Slice := res[1]
		countS := strings.Split(strings.Split(db0Slice, ",")[0], "=")[1]
		count, err = strconv.ParseInt(countS, 10, 0)
		if err != nil {
			return
		}
	}
	total = total + count
	return
}

func (i *Index) GetName() string {
	return i.name
}

// Create configues the index and creates it on redis
func (i *Index) Create() error {
	args := []interface{}{i.commandPrefix + ".CREATE", i.name}
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
			args = append(args, f.Name, "TEXT", "WEIGHT", "1.0")
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

	conn := i.client
	err := conn.Do(context.Background(), args...).Err()
	return err
}

// Index indexes multiple documents on the index, with optional IndexingOptions passed to options
func (i *Index) Index(docs []index.Document, options interface{}) error {
	conn := i.client
	for _, doc := range docs {
		args := []interface{}{"HSET", doc.Id}
		for k, f := range doc.Properties {
			args = append(args, k, f)
		}
		err := conn.Do(context.Background(), args...).Err()
		if err != nil {
			return err
		}
	}
	return nil
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
	conn := i.client
	term := q.Term
	if q.Flags&query.QueryTypePrefix != 0 && term[len(term)-1] != '*' {
		term = fmt.Sprintf("%s*", term)
	}
	queryParam := term
	if q.Field != "" {
		queryParam = fmt.Sprintf("@%s:%s", q.Field, term)
	}
	args := []interface{}{"FT.SEARCH", i.name, queryParam, "LIMIT", q.Paging.Offset, q.Paging.Num, "WITHSCORES"}
	sliceReply, err := conn.Do(context.Background(), args...).Slice()
	if err != nil {
		return
	}
	res := sliceReply[0]
	n := res.(int64)
	total = int(n)
	if verbose > 1 {
		log.Printf(
			"query %v. %d hits",
			args,
			total,
		)
	}
	return nil, total, nil
}

func flush(ctx context.Context, client *goredis.Client) error {
	return client.FlushAll(ctx).Err()
}

func (i *Index) Drop() (err error) {
	if i.cluster {
		err = i.clientClient.ForEachMaster(context.Background(), flush)
	} else {
		err = i.client.FlushDB(context.Background()).Err()
	}
	return
}
