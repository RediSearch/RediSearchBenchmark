package query

// Flag is a type for query flags
type Flag uint64

const (
	QueryVerbatim  Flag = 0x1
	QueryNoContent Flag = 0x2
	// ... more to come!

	DefaultOffset = 0
	DefaultNum    = 10
)

// Query is a single search query and all its parameters and predicates
type Query struct {
	Index      string
	Term       string
	Predicates []Predicate
	Paging     Paging
	Flags      Flag
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
