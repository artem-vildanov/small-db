package table

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

func (c *Column) Serialize(rawData any) ([]byte, error) {
	// todo реализовать сериализацию для каждого типа
	serialized := make([]byte, 0, 10)
	switch c.Type {
	case Int32Type:
	case StringType:
	case BoolType:
	}

	return serialized, nil
}

type Schema struct {
	Columns      []*Column          `json:"columns"`
	NameToColumn map[string]*Column `json:"-"`
}

func CreateNewSchema(path string, columns []*Column) (*Schema, error) {
	// todo save to json
}

func GetSchemaByTableName(name string) *Schema {
	// todo достать из json
}
