package index

type FieldType int

const (
	// TextField: full-text field
	TextField FieldType = iota

	// NumericField: numeric range field
	NumericField

	// GeoField: geo-indexed point field
	GeoField

	// ValueField: as-as short text value to be hashed and indexed
	ValueField

	// NoIndexField: A field that shuold not be indexed
	NoIndexField
)

type Field struct {
	Name    string
	Type    FieldType
	Options interface{}
}

type TextFieldOptions struct {
	Weight   float32
	Stemming bool
}

func NewTextField(name string, weight float32) Field {
	return Field{
		Name: name,
		Type: TextField,
		Options: TextFieldOptions{
			Weight:   1.0,
			Stemming: true,
		},
	}
}

func NewNumericField(name string) Field {
	return Field{
		Name: name,
		Type: NumericField,
	}
}

type Metadata struct {
	Fields  []Field
	Options interface{}
}

func NewMetadata() *Metadata {
	return &Metadata{
		Fields: []Field{},
	}
}

func (m *Metadata) AddField(f Field) *Metadata {
	if m.Fields == nil {
		m.Fields = []Field{}
	}
	m.Fields = append(m.Fields, f)
	return m
}
