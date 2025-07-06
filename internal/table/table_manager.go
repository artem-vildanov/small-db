package table

import "fmt"

const (
	commonPathTemplate = "../../data/%s"
	tablePathTemplate  = commonPathTemplate + ".data"
	schemaPathTemplate = commonPathTemplate + ".json"
)

type TableManager struct {
	NameToTable map[string]*Table
}

func NewTableManager() *TableManager {
	// todo достать метаданные о таблицах и схемах
	return &TableManager{}
}

func (m *TableManager) CreateTable(name string, schema *Schema) (*Table, error) {
	if _, exists := m.NameToTable[name]; exists {
		return nil, ErrTableWithNameExists(name)
	} 

	tablePath := fmt.Sprintf(tablePathTemplate, name)

	table, err := CreateNewTable(tablePath, name, schema)
	if err != nil {
		return nil, fmt.Errorf("CreateNewTable: %w", err)
	}

	m.NameToTable[name] = table

	return table, nil
}

func (m *TableManager) Insert(tableName string, rawRecord map[RawField]any) error {
	table, exists := m.NameToTable[tableName]
	if !exists {
		return ErrTableWithNameDoesntExist(tableName)
	}

	record, err := NewRecordInSchema(table.Schema, rawRecord)
	if err != nil {
		return fmt.Errorf("NewRecordInSchema: %w", err)
	}

	serialized := record.Serialize()

	if err := table.Insert(serialized); err != nil {
		return fmt.Errorf("Table.Insert: %w", err)
	}

	return nil
}

func (m *TableManager) FindByCondition(
	tableName string, 
	match func(*Record) bool,
) (*Record, error) {
	table, exists := m.NameToTable[tableName]
	if !exists {
		return nil, ErrTableWithNameDoesntExist(tableName)
	}

	record, err := table.FindByCondition(match)
	if err != nil {
		return nil, fmt.Errorf("Table.FindByCondition: %w", err)
	}

	return record, nil
}
