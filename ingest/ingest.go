package ingest

import (
	"golang.org/x/exp/slices"
	"regexp"
)
import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/RediSearch/RediSearchBenchmark/index"
)

// DocumentReader implements parsing a data source and yielding documents
type DocumentReader interface {
	Read(io.Reader, chan index.Document, int, index.Index) error
}

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

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

// IngestDocuments ingests documents into an index using a DocumentReader
func ReadTerms(fileName string, r DocumentReader, idx index.Index, chunk int, maxDocsToRead int, maxTermsToProduce int, propertyName string, termStopWords []string) (finalTerms []string, err error) {
	// open the file
	fp, err := os.Open(fileName)
	if err != nil {
		return
	}
	defer fp.Close()
	ch := make(chan index.Document, chunk)
	// run the reader and let it spawn a goroutine
	if err = r.Read(fp, ch, maxDocsToRead, idx); err != nil {
		return
	}
	producedTerms := 0
	finalTerms = make([]string, 0, 0)
	for doc := range ch {
		property := doc.Properties[propertyName].(string)
		property = strings.TrimSpace(property)
		property = nonAlphanumericRegex.ReplaceAllString(property, "")
		terms := strings.Split(property, " ")
		found := false
		term := ""
		try := 0
		maxTries := len(terms)
		for (producedTerms < maxTermsToProduce) && try < maxTries {
			term = terms[rand.Int63n(int64(len(terms)))]
			found = (!(slices.Contains(termStopWords, term)) && len(term) > 1)
			try++
			if found {
				producedTerms++
				finalTerms = append(finalTerms, term)
			}
		}
		if producedTerms > maxTermsToProduce {
			break
		}
	}
	return
}

// IngestDocuments ingests documents into an index using a DocumentReader
func ReadFile(fileName string, r DocumentReader, idx index.Index, opts interface{}, chunk int, maxDocsToRead int, indexingWorkers int) error {

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

	numOfDocs := 0
	for doc := range ch {
		if doc.Id != "" {
			err = idx.Index([]index.Document{doc}, opts)
			if err != nil {
				log.Fatal(err)
			}
			numOfDocs++
		} else {
			fmt.Println("warning empty id")
		}
	}
	return nil
}
