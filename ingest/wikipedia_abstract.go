package ingest

import (
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/RediSearch/RediSearchBenchmark/index"
)

func filter(title, body string) bool {

	if strings.HasPrefix(title, "List of") || strings.HasPrefix(body, "#REDIRECT") || strings.HasPrefix(body, "#redirect") ||
		strings.Contains(title, "(disambiguation)") {
		//fmt.Println(title, body)
		return false
	}

	return true
}

type WikipediaAbstractsReader struct {
	scores   map[string]float64
	topScore float64
}

func (wr *WikipediaAbstractsReader) score(title string) float32 {
	sc := wr.scores[title]

	if wr.topScore == 0 {
		wr.topScore = 1
	}

	return float32(sc / wr.topScore)

}

func NewWikipediaAbstractsReader() *WikipediaAbstractsReader {
	return &WikipediaAbstractsReader{
		scores: map[string]float64{},
	}
}

func (r *WikipediaAbstractsReader) LoadScores(fileName string) error {

	fp, err := os.Open(fileName)
	if err != nil {
		return err
	}

	defer fp.Close()

	csvr := csv.NewReader(fp)
	csvr.Comma = '\t'
	csvr.LazyQuotes = true
	line, err := csvr.Read()
	num := 0
	for err != io.EOF {
		if len(line) == 2 {

			f, err := strconv.ParseFloat(line[1], 32)
			if err == nil {

				r.scores[line[0]] = f
				if f > r.topScore {
					r.topScore = f
				}
				num++
			}

		}
		line, err = csvr.Read()

		if num%500000 == 0 {
			fmt.Println("Loaded", num, "scores")
		}
	}
	if err != io.EOF {
		return err
	}
	return nil
}

func (wr *WikipediaAbstractsReader) Read(r io.Reader, ch chan index.Document, maxDocsToRead int, idx index.Index) error {

	dec := xml.NewDecoder(r)
	go func() {
		docsRead := 1
		tok, err := dec.RawToken()

		props := map[string]string{}
		var currentText string
		for err != io.EOF {

			switch t := tok.(type) {

			case xml.CharData:
				if len(t) > 1 {
					currentText += string(t)
				}

			case xml.EndElement:
				name := t.Name.Local
				if name == "title" || name == "url" || name == "abstract" {
					props[name] = currentText
				} else if name == "doc" {

					id := idx.GetName() + "-" + path.Base(props["url"])
					if len(id) > 0 {
						title := strings.TrimPrefix(strings.TrimSpace(props["title"]), "Wikipedia: ")
						body := strings.TrimSpace(props["abstract"])
						//fmt.Println(title)
						if filter(title, body) {
							doc := index.NewDocument(id, wr.score(id)).
								Set("title", title).
								Set("body", body).
								Set("url", strings.TrimSpace(props["url"]))
								//Set("score", rand.Int31n(50000))
							ch <- doc
							docsRead++
						}
					}
					props = map[string]string{}
				}
				currentText = ""
			}
			if maxDocsToRead != -1 && docsRead >= maxDocsToRead {
				break
			}
			tok, err = dec.RawToken()

		}
		close(ch)
	}()
	return nil
}
