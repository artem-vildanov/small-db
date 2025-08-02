package schema

type ColumnType string

const (
	StringType ColumnType = "string"
	Int32Type  ColumnType = "int32"
	BoolType   ColumnType = "bool"
)

var DynamicMemoTypes = []ColumnType{
	StringType,
}

const (
	DynamicMemoTypeColumnSize = -1
)

type ColumnSize int

// размеры в байтах
const (
	Int32Size ColumnSize = 4
	BoolSize  ColumnSize = 1
)

type Column struct {
	Name     string     `json:"name"`
	Type     ColumnType `json:"type"`
	Size     int        `json:"size"`
	Nullable bool       `json:"nullable"`
}

type Schema struct {
	ID           string             `json:"id"`
	Hash         string             `json:"hash"` // 32 bytes
	Columns      []*Column          `json:"columns"`
	PrimaryKeys  []string           `json:"primaryKeys"`
	NameToColumn map[string]*Column `json:"-"`
}
