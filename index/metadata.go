package index

// FieldType is an enumeration of field/property types
type FieldType int

const (
	// TextField full-text field
	TextField FieldType = iota

	// NumericField numeric range field
	NumericField

	// GeoField geo-indexed point field
	GeoField

	// ValueField as-as short text value to be hashed and indexed
	ValueField

	// NoIndexField A field that shuold not be indexed
	NoIndexField
)

// Field represents a single field's metadata
type Field struct {
	Name    string
	Type    FieldType
	Options interface{}
}

// TextFieldOptions Options for text fields - weight and stemming enabled/disabled.
type TextFieldOptions struct {
	Weight   float32
	Stemming bool
}

// NewTextField creates a new text field with the given weight
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

// NewNumericField creates a new numeric field with the given name
func NewNumericField(name string) Field {
	return Field{
		Name: name,
		Type: NumericField,
	}
}

// Metadata represents an index schema metadata, or how the index would
// treat documents sent to it.
type Metadata struct {
	Fields  []Field
	Options interface{}
}

// NewMetadata creates a new Metadata object
func NewMetadata() *Metadata {
	return &Metadata{
		Fields: []Field{},
	}
}

// AddField adds a field to the Metadata object
func (m *Metadata) AddField(f Field) *Metadata {
	if m.Fields == nil {
		m.Fields = []Field{}
	}
	m.Fields = append(m.Fields, f)
	return m
}
