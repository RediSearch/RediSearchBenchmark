package ingest

import (
	"compress/bzip2"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/RediSearch/RediSearchBenchmark/index"
)

//
//"name": "20_Century_Br_Hist_2015_Sep_27_26(3)_450-476",
//"journal": "20 Century Br Hist",
//"date": "2015 Sep 27",
//"volume": "26(3)",
//"issue": "450-476",
//"accession": "PMC4804230",
//"timestamp": "2016-03-24 20:08:28",
//"pmid": "",
//"body"

// we've mapped the index definitions from
//https://github.com/elastic/rally-tracks/blob/master/pmc/index.json
type pmcDocument struct {
	Name      string `json:"name"`
	Journal   string `json:"journal"`
	Date      string `json:"date"`
	Volume    string `json:"volume"`
	Issue     string `json:"issue"`
	Accession string `json:"accession"`
	Timestamp string `json:"timestamp"`
	Pmid      string `json:"pmid"`
	Body      string `json:"body"`
}

type PmcReader struct{}

func (rr *PmcReader) Read(r io.Reader, ch chan index.Document, maxDocsToRead int, idx index.Index) error {
	log.Println("pmc reader opening", r)
	bz := bzip2.NewReader(r)
	jr := json.NewDecoder(bz)
	//layout := "YYYY-MM-DDThh:mm:ss"

	var rd pmcDocument
	var err error
	go func() {
		for err != io.EOF {

			if err := jr.Decode(&rd); err != nil {
				log.Printf("Error decoding json: %v", err)
				break
			}
			docid := fmt.Sprintf("%s:%s:%s", rd.Journal, rd.Volume, rd.Name)
			ts := rd.Timestamp[0:10] + "T" + rd.Timestamp[11:] + "Z"
			timeStamp, err := time.Parse(time.RFC3339, ts)
			if err != nil {
				log.Printf("Error decoding timestamp %s. Error: %v", rd.Timestamp, err)
			}

			doc := index.NewDocument(docid, 1.0).
				Set("name", rd.Name).
				Set("journal", rd.Journal).
				Set("date", rd.Date).
				Set("volume", rd.Volume).
				Set("issue", rd.Issue).
				Set("accession", rd.Accession).
				Set("timestamp", timeStamp.Unix()).
				Set("accession", rd.Accession).
				Set("pmid", rd.Pmid).
				Set("body", rd.Body)
			ch <- doc
		}
		close(ch)
	}()
	return nil
}
