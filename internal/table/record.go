package table

import (
	"encoding/binary"
	"fmt"
)

// размер в байтах префикса, который добавляется перед
// значением с динамическим размером.
// префикс содержит фактический размер значения
const DynamicValuePrefixSize = 2

type Record struct {
	Fields            []*Field
	ColumnNameToField map[string]*Field
}

func NewRecordInSchema(schema *Schema, rawRecord map[string]any) (*Record, error) {
	columnNameToField := make(map[string]*Field, len(schema.Columns))
	for inputColumnName, inputValue := range rawRecord {
		column, exists := schema.NameToColumn[inputColumnName]
		if !exists {
			return nil, ErrNoSuchColumnInSchema(column.Name)
		}

		serializedValue, err := column.Serialize(inputValue)
		if err != nil {
			return nil, fmt.Errorf("Column.Serialize: %w", err)
		}

		columnNameToField[column.Name] = &Field{
			Column: column,
			Value:  serializedValue,
		}
	}

	record := &Record{
		Fields: make([]*Field, 0, len(rawRecord)),
	}

	for _, column := range schema.Columns {
		field, exists := columnNameToField[column.Name]
		// todo реализовать default value
		if !exists {
			return nil, ErrFieldNotProvided(column.Name)
		}

		record.Fields = append(record.Fields, field)
	}

	record.ColumnNameToField = columnNameToField

	return record, nil
}

func DeserializeRecordBySchema(schema *Schema, data []byte) *Record {
	var offset int
	record := &Record{
		Fields: make([]*Field, 0, len(schema.Columns)),
		ColumnNameToField: make(map[string]*Field, len(schema.Columns)),
	}

	for _, column := range schema.Columns {
		var size int

		isDynamicMemoType := column.Size == DynamicMemoTypeColumnSize
		if isDynamicMemoType {
			size = int(
				binary.BigEndian.Uint16(data[offset:DynamicValuePrefixSize]),
			)
			offset += DynamicValuePrefixSize
		} else {
			size = column.Size
		}

		field := &Field{
			Column: column,
			Value:  data[offset : offset+size],
		}

		record.Fields = append(record.Fields, field)
		record.ColumnNameToField[column.Name] = field

		offset += size
	}

	return record
}

func (r *Record) Serialize() []byte {
	var serializedLen int

	for _, field := range r.Fields {
		isDynamicMemoType := field.Column.Size == DynamicMemoTypeColumnSize
		if isDynamicMemoType {
			serializedLen += len(field.Value) + DynamicValuePrefixSize
		} else {
			serializedLen += field.Column.Size
		}
	}

	serialized := make([]byte, 0, serializedLen)

	for _, field := range r.Fields {
		isDynamicMemoType := field.Column.Size == DynamicMemoTypeColumnSize
		if isDynamicMemoType {
			// добавляем в начало значения префикс с длиной
			// размер префикса - 2 байта
			var prefix []byte
			binary.BigEndian.PutUint16(prefix, uint16(len(field.Value)))
			field.Value = append(prefix, field.Value...)
		}

		serialized = append(serialized, field.Value...)
	}

	return serialized
}

type Field struct {
	Column *Column
	Value  []byte
}

type RawField struct {
	Type string
	Name string
}
