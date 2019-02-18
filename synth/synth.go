package synth

import (
	"math/rand"

	"fmt"

	"strings"

	"time"

	"github.com/RediSearch/RediSearchBenchmark/index"
)

// DocumentGenerator generates synthetic documents for benchmarkig
type DocumentGenerator struct {
	// mapping of field names and min/max tokens per field
	fields map[string][2]int

	vocabSize int

	maxDocId int

	rng *rand.Zipf
}

func NewDocumentGenerator(vocabSize int, fields map[string][2]int) *DocumentGenerator {

	rng := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), 1.0001, 20, uint64(vocabSize))

	gen := &DocumentGenerator{
		fields:    fields,
		vocabSize: vocabSize,
		maxDocId:  1,
		rng:       rng,
	}

	return gen
}

// Generate generates a synthetic document with a given id. If id is 0, we select an incremental id
func (g *DocumentGenerator) Generate(docId int) index.Document {
	if docId == 0 {
		docId = g.maxDocId
		g.maxDocId++
	}
	doc := index.NewDocument(fmt.Sprintf("doc%d", docId), 1.0)
	for f, tokrange := range g.fields {
		ntoks := rand.Intn(tokrange[1]-tokrange[0]) + tokrange[0]
		toks := make([]string, ntoks)
		for i := 0; i < ntoks; i++ {
			toks[i] = fmt.Sprintf("term%d", g.rng.Uint64())
		}
		doc.Set(f, strings.Join(toks, " "))
	}
	// gen := rng.NewGeometricGenerator(time.Now().UnixNano())
	// for i := 0; i < 1000; i++ {
	// 	fmt.Println(gen.Geometric(0.1))
	// }
	return doc
}
