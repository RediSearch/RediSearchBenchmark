package index

import "sort"

// Suggestion is a single suggestion being added or received from the Autocompleter
type Suggestion struct {
	Term  string
	Score float64
}

// Autocompleter is an abstract interface for all auto-completers implemented on all engines
type Autocompleter interface {
	AddTerms(terms ...Suggestion) error
	Suggest(prefix string, num int, fuzzy bool) ([]Suggestion, error)
	Delete() error
}

// SuggestionList is a sortable list of suggestions returned from an engine
type SuggestionList []Suggestion

func (l SuggestionList) Len() int           { return len(l) }
func (l SuggestionList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l SuggestionList) Less(i, j int) bool { return l[i].Score > l[j].Score } //reverse sorting

// Sort the SuggestionList
func (l SuggestionList) Sort() {
	sort.Sort(l)
}
