package index

type Document struct {
	Id         string
	Score      float32
	Properties map[string]interface{}
}

func NewDocument(id string, score float32) Document {
	return Document{
		Id:         id,
		Score:      score,
		Properties: make(map[string]interface{}),
	}
}

func (d Document) Set(name string, value interface{}) Document {
	d.Properties[name] = value
	return d
}
