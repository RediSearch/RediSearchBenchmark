package synth

import (
	"fmt"
	"testing"
)

func TestDocumentGenerator(t *testing.T) {
	g := NewDocumentGenerator(1000, map[string][2]int{"title": {5, 10}, "body": {10, 50}})
	for i := 0; i < 100; i++ {
		doc := g.Generate(0)
		if doc.Id == "" {
			t.Fail()
		}
		fmt.Printf("%#v\n", doc)
	}
}

func BenchmarkGenerator(b *testing.B) {
	g := NewDocumentGenerator(1000, map[string][2]int{"title": {10, 15}})
	for i := 0; i < b.N; i++ {
		_ = g.Generate(0)
		//fmt.Printf("%#v\n", doc)
	}
}
