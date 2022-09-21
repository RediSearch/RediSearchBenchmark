package query

// Flag is a type for query flags
type Flag uint64

const (
	QueryVerbatim     Flag = 0x1
	QueryNoContent    Flag = 0x2
	QueryTypePrefix   Flag = 0x3
	QueryTypeWildcard Flag = 0x4
	// ... more to come!

	DefaultOffset = 0
	DefaultNum    = 10
)

// HighlightOptions represents the options to higlight specific document fields.
// See http://redisearch.io/Highlight/
type HighlightOptions struct {
	Fields []string
	Tags   [2]string
}

// SummaryOptions represents the configuration used to create field summaries.
// See http://redisearch.io/Highlight/
type SummaryOptions struct {
	Fields       []string
	FragmentLen  int    // default 20
	NumFragments int    // default 3
	Separator    string // default "..."
}

// Query is a single search query and all its parameters and predicates
type Query struct {
	Index      string
	Term       string
	Field      string
	Predicates []Predicate
	Paging     Paging
	Flags      Flag

	HighlightOpts *HighlightOptions
	SummarizeOpts *SummaryOptions
}

// Paging represents the offset paging of a search result
type Paging struct {
	Offset int
	Num    int
}

// NewQuery creates a new query for a given index with the given search term.
// For currently the index parameter is ignored
func NewQuery(index, term string) *Query {
	return &Query{
		Index:      index,
		Term:       term,
		Predicates: []Predicate{},
		Paging:     Paging{DefaultOffset, DefaultNum},
	}
}

// AddPredicate adds a predicate to the query's filters
func (q *Query) AddPredicate(p Predicate) *Query {
	q.Predicates = append(q.Predicates, p)
	return q
}

// Limit sets the paging offset and limit for the query
func (q *Query) Limit(offset, num int) *Query {
	q.Paging.Offset = offset
	q.Paging.Num = num
	return q
}

// SetFlags sets the query's optional flags
func (q *Query) SetFlags(flags Flag) *Query {
	q.Flags = flags
	return q
}

// Highlight sets highighting on given fields. Highlighting marks all the query terms
// with the given open and close tags (i.e. <b> and </b> for HTML)
func (q *Query) Highlight(fields []string, openTag, closeTag string) *Query {
	q.HighlightOpts = &HighlightOptions{
		Fields: fields,
		Tags:   [2]string{openTag, closeTag},
	}
	return q
}

// Summarize sets summarization on the given list of fields.
// It will instruct the engine to extract the most relevant snippets
// from the fields and return them as the field content.
// This function works with the default values of the engine, and only sets the fields.
// There is a function that accepts all options - SummarizeOptions
func (q *Query) Summarize(fields ...string) *Query {

	q.SummarizeOpts = &SummaryOptions{
		Fields: fields,
	}
	return q
}

// SummarizeOptions sets summarization on the given list of fields.
// It will instruct the engine to extract the most relevant snippets
// from the fields and return them as the field content.
//
// This function accepts advanced settings for snippet length, separators and number of snippets
func (q *Query) SummarizeOptions(opts SummaryOptions) *Query {
	q.SummarizeOpts = &opts
	return q
}

func (q *Query) SetField(field string) *Query {
	q.Field = field
	return q
}
