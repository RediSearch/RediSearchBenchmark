package ingest

import (
	"fmt"
	"io"
	"os"

	"encoding/json"

	"github.com/RedisLabs/RediSearchBenchmark/index"
)

type redditDocument struct {
	Author     string  `json:"author"`
	Body       string  `json:"body"`
	Created    string  `json:"created_utc"`
	Id         string  `json:"id"`
	Score      int64   `json:"score"`
	Ups        int64   `json:"ups"`
	Downs      int64   `json:"downs"`
	Subreddit  string  `json:"subreddit"`
	UvoteRatio float32 `json:"upvote_ratio"`
}

type RedditReader struct{}

func (rr *RedditReader) Read(r io.Reader) (<-chan index.Document, error) {

	jr := json.NewDecoder(r)

	var rd redditDocument

	ch := make(chan index.Document, 1000)
	go func() {
		var err error

		for err != io.EOF {

			if err := jr.Decode(&rd); err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			doc := index.NewDocument(rd.Id, float32(rd.Score)).
				Set("body", rd.Body).
				Set("author", rd.Author).
				Set("sub", rd.Subreddit).
				Set("date", rd.Created).
				Set("ups", rd.Ups)

			ch <- doc
		}
		close(ch)
	}()
	return ch, nil
}
