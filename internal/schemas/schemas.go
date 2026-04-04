package schemas

type SchemaID string

type Schema struct {
	ID      SchemaID
	Bytes   []byte
	Version int
}
