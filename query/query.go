package query

type Flag uint64

const (
	QueryVerbatim  Flag = 0x1
	QueryNoContent Flag = 0x2
	// ... more to come!

	DefaultOffset = 0
	DefaultNum    = 10
)

type Query struct {
	Index      string
	Term       string
	Predicates []Predicate
	Paging     Paging
	Flags      Flag
}

type Paging struct {
	Offset int
	Num    int
}

func NewQuery(index, term string) *Query {
	return &Query{
		Index:      index,
		Term:       term,
		Predicates: []Predicate{},
		Paging:     Paging{DefaultOffset, DefaultNum},
	}
}

func (q *Query) AddPredicate(p Predicate) *Query {
	q.Predicates = append(q.Predicates, p)
	return q
}

func (q *Query) Limit(offset, num int) *Query {
	q.Paging.Offset = offset
	q.Paging.Num = num
	return q
}

func (q *Query) SetFlags(flags Flag) *Query {
	q.Flags = flags
	return q
}
