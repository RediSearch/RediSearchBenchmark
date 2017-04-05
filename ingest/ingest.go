package ingest

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/RedisLabs/RediSearchBenchmark/index"
)

// DocumentReader implements parsing a data source and yielding documents
type DocumentReader interface {
	Read(io.Reader) (<-chan index.Document, error)
}

// IngestDocuments ingests documents into an index using a DocumentReader
func IngestDocuments(fileName string, r DocumentReader, idx index.Index, ac index.Autocompleter, opts interface{}, chunk int) error {

	// open the file
	fp, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer fp.Close()

	// run the reader and let it spawn a goroutine
	ch, err := r.Read(fp)
	if err != nil {
		return err
	}

	docs := make([]index.Document, chunk*2)
	terms := make([]index.Suggestion, chunk*2)

	//freqs := map[string]int{}
	st := time.Now()

	nterms := 0
	i := 0
	n := 0
	dt := 0
	totalDt := 0
	doch := make(chan index.Document, 100)
	for w := 0; w < 200; w++ {
		go func(doch chan index.Document) {
			for doc := range doch {
				if doc.Id != "" {
					//fmt.Println(doc)
					idx.Index([]index.Document{doc}, opts)
				}
			}
		}(doch)
	}
	for doc := range ch {

		docs[i%chunk] = doc

		if doc.Score > 0 && ac != nil {

			//			words := strings.Fields(strings.ToLower(doc.Properties["body"].(string)))
			//			for _, w := range words {
			//				for i := 2; i < len(w) && i < 5; i++ {
			//					freqs[w[:i]] += 1
			//				}
			//			}

			terms[nterms] = index.Suggestion{
				strings.ToLower(doc.Properties["title"].(string)),
				float64(doc.Score),
			}
			nterms++

			if nterms == chunk {

				//				for k, v := range freqs {
				//					fmt.Printf("%d %s\n", v, k)
				//				}
				//				os.Exit(0)
				/*if err := ac.AddTerms(terms...); err != nil {
					return err
				}*/
				nterms = 0
			}

		}
		if doc.Score == 0 {
			doc.Score = 0.0000001
		}

		for k, v := range doc.Properties {
			switch s := v.(type) {
			case string:
				dt += len(s) + len(k)
				totalDt += len(s) + len(k)
			}
		}

		i++
		n++
		if i%chunk == 0 {
			//var _docs []index.Document
			for _, d := range docs {
				doch <- d
			}

		}

		// print report every CHUNK documents
		if i%chunk == 0 {
			rate := float32(n) / (float32(time.Since(st).Seconds()))
			dtrate := float32(dt) / (float32(time.Since(st).Seconds())) / float32(1024*1024)
			fmt.Println(i, "rate: ", rate, "d/s. data rate: ", dtrate, "MB/s", "total data ingested", float32(totalDt)/float32(1024*1024))
			st = time.Now()
			n = 0
			dt = 0
		}
	}

	if i%chunk != 0 {
		go idx.Index(docs[:i%chunk], opts)
		return nil
	}
	return nil
}
