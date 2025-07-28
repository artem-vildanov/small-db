package table

import (
	"encoding/binary"
	"fmt"

	"github.com/artem-vildanov/small-db/internal/schema"
)

const (
	// размер в байтах префикса, который добавляется перед
	// значением с динамическим размером.
	// префикс содержит фактический размер значения
	DynamicValuePrefixSize = 2

	FalseByte byte = 0
	TrueByte  byte = 1
)

type Record struct {
	Fields            []*Field
	ColumnNameToField map[string]*Field
}

func NewEmptyRecord() *Record {
	return &Record{
		Fields: make([]*Field, 0),
		ColumnNameToField: make(map[string]*Field),
	}
}

func (r *Record) addFields(fields ...*Field) {
	for _, field := range fields {
		r.Fields = append(r.Fields, field)
		r.ColumnNameToField[field.Column.Name] = field
	}
}

func (r *Record) IntoNameToValue() (map[string]any, error) {
	nameToValue := make(map[string]any, len(r.Fields))
	for _, field := range r.Fields {
		v, err := deserializeValue(field.Column.Type, field.Value)
		if err != nil {
			return nil, fmt.Errorf("deserializeValue: %w", err)
		}

		nameToValue[field.Column.Name] = v
	}

	return nameToValue, nil
}

func (r *Record) GetInt32FieldValue(fieldName string) (int32, error) {
	field, exists := r.ColumnNameToField[fieldName]
	if !exists {
		return 0, ErrNoSuchColumnInSchema(fieldName)
	}

	deserialized, err := deserializeValue(field.Column.Type, field.Value)
	if err != nil {
		return 0, fmt.Errorf("deserializeValue: %w", err)
	}

	casted, ok := deserialized.(int32)
	if !ok {
		return 0, ErrFailedToCast(field.Column.Type)
	}

	return casted, nil
}

func (r *Record) GetStringFieldValue(fieldName string) (string, error) {
	field, exists := r.ColumnNameToField[fieldName]
	if !exists {
		return "", ErrNoSuchColumnInSchema(fieldName)
	}

	deserialized, err := deserializeValue(field.Column.Type, field.Value)
	if err != nil {
		return "", fmt.Errorf("deserializeValue: %w", err)
	}

	casted, ok := deserialized.(string)
	if !ok {
		return "", ErrFailedToCast(field.Column.Type)
	}

	return casted, nil
}

func (r *Record) GetBoolFieldValue(fieldName string) (bool, error) {
	field, exists := r.ColumnNameToField[fieldName]
	if !exists {
		return false, ErrNoSuchColumnInSchema(fieldName)
	}

	deserialized, err := deserializeValue(field.Column.Type, field.Value)
	if err != nil {
		return false, fmt.Errorf("deserializeValue: %w", err)
	}

	casted, ok := deserialized.(bool)
	if !ok {
		return false, ErrFailedToCast(field.Column.Type)
	}

	return casted, nil
}

func NewRecordInSchema(schema *schema.Schema, rawRecord map[string]any) (*Record, error) {
	columnNameToField := make(map[string]*Field, len(schema.Columns))
	for inputColumnName, inputValue := range rawRecord {
		column, exists := schema.NameToColumn[inputColumnName]
		if !exists {
			return nil, ErrNoSuchColumnInSchema(inputColumnName)
		}

		serializedValue, err := serializeValue(column.Type, inputValue)
		if err != nil {
			return nil, fmt.Errorf("serializeValue: %w", err)
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

func serializeValue(columnType schema.ColumnType, raw any) ([]byte, error) {
	switch columnType {
	case schema.Int32Type:
		return serializeInt32(raw)
	case schema.StringType:
		return serializeString(raw)
	case schema.BoolType:
		return serializeBool(raw)
	default:
		return nil, ErrUnexpectedType(columnType)
	}
}

func serializeInt32(raw any) ([]byte, error) {
	serialized := make([]byte, 4)
	intVal, ok := raw.(int32)
	if !ok {
		return nil, ErrFailedToSerialize(schema.Int32Type)
	}

	binary.BigEndian.PutUint32(serialized, uint32(intVal))
	return serialized, nil
}

func serializeString(raw any) ([]byte, error) {
	strVal, ok := raw.(string)
	if !ok {
		return nil, ErrFailedToSerialize(schema.StringType)
	}

	return []byte(strVal), nil
}

func serializeBool(raw any) ([]byte, error) {
	boolVal, ok := raw.(bool)
	if !ok {
		return nil, ErrFailedToSerialize(schema.BoolType)
	}

	if boolVal {
		return []byte{TrueByte}, nil
	} else {
		return []byte{FalseByte}, nil
	}
}

func deserializeValue(columnType schema.ColumnType, raw []byte) (any, error) {
	switch columnType {
	case schema.Int32Type:
		return deserializeInt32(raw)
	case schema.StringType:
		return deserializeString(raw)
	case schema.BoolType:
		return deserializeBool(raw)
	default:
		return nil, ErrUnexpectedType(columnType)
	}
}

func deserializeInt32(raw []byte) (int32, error) {
	return int32(binary.BigEndian.Uint32(raw)), nil
}

func deserializeString(raw []byte) (string, error) {
	return string(raw), nil
}

func deserializeBool(raw []byte) (bool, error) {
	if len(raw) != 1 {
		return false, ErrFailedToDeserializeBool(len(raw))
	}

	return raw[0] == TrueByte, nil
}

func ErrUnexpectedType(t schema.ColumnType) error {
	return fmt.Errorf("got unexpected type %s", t)
}

func ErrFailedToCast(t schema.ColumnType) error {
	return fmt.Errorf("failed to cast deserialized value into type %s", t)
}

func ErrFailedToSerialize(t schema.ColumnType) error {
	return fmt.Errorf("failed to serialize %s value", t)
}

func ErrFailedToDeserializeBool(actualBoolLen int) error {
	return fmt.Errorf("failed to deserialize bool value: got unexpected value len %d", actualBoolLen)
}

func DeserializeRecordBySchema(bySchema *schema.Schema, data []byte) *Record {
	var offset int
	record := &Record{
		Fields:            make([]*Field, 0, len(bySchema.Columns)),
		ColumnNameToField: make(map[string]*Field, len(bySchema.Columns)),
	}

	for _, column := range bySchema.Columns {
		var size int

		isDynamicMemoType := column.Size == schema.DynamicMemoTypeColumnSize
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
		isDynamicMemoType := field.Column.Size == schema.DynamicMemoTypeColumnSize
		if isDynamicMemoType {
			serializedLen += len(field.Value) + DynamicValuePrefixSize
		} else {
			serializedLen += field.Column.Size
		}
	}

	serialized := make([]byte, 0, serializedLen)

	for _, field := range r.Fields {
		isDynamicMemoType := field.Column.Size == schema.DynamicMemoTypeColumnSize
		if isDynamicMemoType {
			// добавляем в начало значения префикс с длиной
			// размер префикса - 2 байта
			prefix := make([]byte, 2)
			binary.BigEndian.PutUint16(prefix, uint16(len(field.Value)))
			field.Value = append(prefix, field.Value...)
		}

		serialized = append(serialized, field.Value...)
	}

	return serialized
}

type Field struct {
	Column *schema.Column
	Value  []byte
}
