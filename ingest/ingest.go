package ingest

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sync"

	"github.com/RediSearch/RediSearchBenchmark/index"
)

// DocumentReader implements parsing a data source and yielding documents
type DocumentReader interface {
	Read(io.Reader, chan index.Document, int, index.Index) error
}

func walkDir(path string, pattern string, ch chan string) {

	files, err := ioutil.ReadDir(path)

	if err != nil {
		log.Printf("Could not read path %s: %s", path, err)
		panic(err)
	}

	for _, file := range files {
		fullpath := filepath.Join(path, file.Name())
		if file.IsDir() {
			walkDir(fullpath, pattern, ch)
			continue
		}

		if match, err := filepath.Match(pattern, file.Name()); err == nil {

			if match {
				log.Println("Reading ", fullpath)
				ch <- fullpath
			}
		} else {
			panic(err)
		}

	}
}

type Stats struct {
	TotalDocs             int64
	CurrentWindowDocs     int
	CurrentWindowDuration time.Duration
	CurrentWindowRate     float64
	CurrentWindowLatency  time.Duration
}

func ngrams(words []string, size int, count map[string]uint32) {

	offset := int(math.Floor(float64(size / 2)))

	max := len(words)
	for i := range words {
		if i < offset || i+size-offset > max {
			continue
		}
		gram := strings.Join(words[i-offset:i+size-offset], " ")
		count[gram] += uint32(size)
	}

}

// ReadDir reads a complete directory and feeds each file it finds to a document reader
func ReadDir(dirName string, pattern string, r DocumentReader, idx index.Index, ac index.Autocompleter,
	opts interface{}, chunk int, workers int, conns int, stats chan Stats, maxDocsToRead int) {
	filech := make(chan string, 100)
	go func() {
		defer close(filech)
		walkDir(dirName, pattern, filech)
	}()

	doch := make(chan index.Document, chunk)
	countch := make(chan time.Duration, chunk*workers)
	// start the independent idexing workers
	wg := sync.WaitGroup{}
	go func() {
		for i := 0; i < conns; i++ {
			wg.Add(1)
			go func(doch chan index.Document, countch chan time.Duration) {
				for doc := range doch {
					if doc.Id != "" {
						//fmt.Println(doc)
						st := time.Now()
						idx.Index([]index.Document{doc}, opts)
						dur := time.Since(st)
						countch <- dur

					}
				}
				wg.Done()
			}(doch, countch)

		}
		wg.Wait()
	}()
	// start the file reader workers
	for i := 0; i < workers; i++ {
		go func(filech chan string, doch chan index.Document) {
			for file := range filech {
				fp, err := os.Open(file)
				if err != nil {
					log.Println(err)
				} else {
					if err = r.Read(fp, doch, maxDocsToRead, idx); err != nil {
						log.Printf("Error reading %s: %s", file, err)
					}
				}
				fp.Close()
			}
		}(filech, doch)
	}

	stt := Stats{
		CurrentWindowDocs:     0,
		TotalDocs:             0,
		CurrentWindowRate:     0,
		CurrentWindowDuration: 0,
		CurrentWindowLatency:  0,
	}

	st := time.Now()
	var totalLatency time.Duration
	for rtt := range countch {
		stt.TotalDocs++
		stt.CurrentWindowDocs++
		totalLatency += rtt

		if time.Since(st) > 200*time.Millisecond {
			stt.CurrentWindowDuration = time.Since(st)
			stt.CurrentWindowRate = float64(stt.CurrentWindowDocs) / (float64(stt.CurrentWindowDuration.Seconds()))
			stt.CurrentWindowLatency = totalLatency / (1 + time.Duration(stt.CurrentWindowDocs))
			//dtrate := float32(dt) / (float32(time.Since(st).Seconds())) / float32(1024*1024)
			fmt.Println(stt.TotalDocs, "docs done, avg latency:", stt.CurrentWindowLatency, " rate: ", stt.CurrentWindowRate, "d/s")
			st = time.Now()
			if stats != nil {
				stats <- stt
			}
			stt.CurrentWindowDocs = 0
			totalLatency = 0

		}
	}
	fmt.Println("Done!")
}

// IngestDocuments ingests documents into an index using a DocumentReader
func ReadFile(fileName string, r DocumentReader, idx index.Index, ac index.Autocompleter,
	opts interface{}, chunk int, maxDocsToRead int) error {

	var wg sync.WaitGroup

	// open the file
	fp, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer fp.Close()
	ch := make(chan index.Document, chunk)
	// run the reader and let it spawn a goroutine
	if err := r.Read(fp, ch, maxDocsToRead, idx); err != nil {
		return err
	}

	st := time.Now()

	i := 0
	n := 0
	dt := 0
	totalDt := 0
	doch := make(chan index.Document, 1)
	for w := 0; w < 200; w++ {
		wg.Add(1)
		go func(doch chan index.Document) {
			defer wg.Done()
			docs := []index.Document{}
			numOfDocs := 0
			for doc := range doch {
				if doc.Id != "" {
					docs = append(docs, doc)
					numOfDocs++;
				}else{
					fmt.Println("warning empty id")
				}
				if(len(docs) > 1000){
					idx.Index(docs, opts)
					docs = []index.Document{}
				}
			}
			if(len(docs) > 0){
				idx.Index(docs, opts)
			}
		}(doch)
	}
	for doc := range ch {

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
		doch <- doc

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

	close(doch)
	wg.Wait()

	return nil
}
