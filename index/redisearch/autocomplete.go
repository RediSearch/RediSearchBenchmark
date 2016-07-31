package redisearch

import (
	"strconv"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	"github.com/garyburd/redigo/redis"
)

type Autocompleter struct {
	pool *redis.Pool
	name string
}

func NewAutocompleter(addr, name string) *Autocompleter {
	return &Autocompleter{
		pool: redis.NewPool(func() (redis.Conn, error) {
			return redis.Dial("tcp", addr)
		}, MaxConns),
		name: name,
	}
}

func (a *Autocompleter) Delete() error {

	conn := a.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", a.name)
	return err
}

func (a *Autocompleter) AddTerms(terms ...index.Suggestion) error {

	conn := a.pool.Get()
	defer conn.Close()

	i := 0
	for _, term := range terms {
		if err := conn.Send("FT.SUGADD", a.name, term.Term, term.Score); err != nil {
			return err
		}
		i += 1
	}
	if err := conn.Flush(); err != nil {
		return err
	}
	for i > 0 {
		if _, err := conn.Receive(); err != nil {
			return err
		}
		i--
	}
	return nil
}
func (a *Autocompleter) Suggest(prefix string, num int, fuzzy bool) ([]index.Suggestion, error) {
	conn := a.pool.Get()
	defer conn.Close()

	args := redis.Args{a.name, prefix, "MAX", num, "WITHSCORES"}
	if fuzzy {
		args = append(args, "FUZZY")
	}
	vals, err := redis.Strings(conn.Do("FT.SUGGET", args...))
	if err != nil {
		return nil, err
	}

	ret := make([]index.Suggestion, 0, len(vals)/2)
	for i := 0; i < len(vals); i += 2 {

		score, err := strconv.ParseFloat(vals[i+1], 64)
		if err != nil {
			continue
		}
		ret = append(ret, index.Suggestion{vals[i], score})

	}

	return ret, nil

}
