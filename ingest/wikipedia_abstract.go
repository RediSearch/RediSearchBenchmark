package ingest

import (
	"encoding/xml"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/RedisLabs/RediSearchBenchmark/index"
)

func filter(title, body string) bool {

	if strings.HasPrefix(title, "List of") || strings.HasPrefix(body, "#REDIRECT") || strings.HasPrefix(body, "#redirect") ||
		strings.Contains(title, "(disambiguation)") {
		//fmt.Println(title, body)
		return false
	}

	return true
}
func ReadWikipediaExtracts(r io.Reader) (<-chan index.Document, error) {

	dec := xml.NewDecoder(r)
	ch := make(chan index.Document)
	go func() {

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

					id := path.Base(props["url"])
					if len(id) > 0 {
						title := strings.TrimPrefix(strings.TrimSpace(props["title"]), "Wikipedia: ")
						body := strings.TrimSpace(props["abstract"])
						//fmt.Println(title)
						if filter(title, body) {
							doc := index.NewDocument(id, 1.0).
								Set("title", title).
								Set("body", body).
								Set("url", strings.TrimSpace(props["url"]))
							ch <- doc
						}
					}
					props = map[string]string{}
				}
				currentText = ""
			}
			tok, err = dec.RawToken()

		}
		fmt.Println("error: ", err)
		close(ch)
	}()
	return ch, nil
}
