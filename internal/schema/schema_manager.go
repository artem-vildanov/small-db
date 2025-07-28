package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/artem-vildanov/small-db/internal/consts"
	"github.com/google/uuid"
)

type SchemaManager struct {
	schemasDirPath string
	IdToSchema     map[string]*Schema
}

func InitSchemaManager(schemasDirPath string) (*SchemaManager, error) {
	entries, err := os.ReadDir(schemasDirPath)
	if err != nil {
		return nil, fmt.Errorf("NewSchemaManager: %w", err)
	}

	idToSchema := make(map[string]*Schema, len(entries))
	for _, entry := range entries {
		isFile := entry.Type().IsRegular()
		isJson := filepath.Ext(entry.Name()) == consts.JsonExtension

		if !isFile || !isJson {
			continue
		}

		fullPath := fmt.Sprintf("%s%s", schemasDirPath, entry.Name())

		rawData, err := os.ReadFile(fullPath)
		if err != nil {
			return nil, fmt.Errorf("os.ReadFile: %w", err)
		}

		var schema Schema
		if err := json.Unmarshal(rawData, &schema); err != nil {
			return nil, fmt.Errorf("json.Unmarshal: %w", err)
		}

		idToSchema[schema.ID] = &schema
	}

	return &SchemaManager{
		schemasDirPath: schemasDirPath,
		IdToSchema:     idToSchema,
	}, nil
}

func (m *SchemaManager) CreateNewSchema(columns []*Column) (*Schema, error) {
	schemaID := uuid.NewString()
	schemaFilePath := fmt.Sprintf(
		getSchemaFilePathTemplate(m.schemasDirPath),
		schemaID,
	)

	hash, err := m.hashColumns(columns)
	if err != nil {
		return nil, fmt.Errorf("hashColumns: %w", err)
	}

	nameToColumn := make(map[string]*Column, len(columns))
	for _, column := range columns {
		nameToColumn[column.Name] = column
	}

	schema := &Schema{
		ID:           schemaID,
		Hash:         hash,
		Columns:      columns,
		NameToColumn: nameToColumn,
	}

	marshalledSchema, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("CreateNewSchema: %w", err)
	}

	descriptor, err := os.OpenFile(
		schemaFilePath, 
		consts.CreateIfNotExists, 
		consts.PosixAccessRight,
	)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}
	defer descriptor.Close()

	if _, err := descriptor.Write(marshalledSchema); err != nil {
		return nil, fmt.Errorf("File.Write: %w", err)
	}

	m.IdToSchema[schema.ID] = schema

	return schema, nil
}

func (m *SchemaManager) hashColumns(columns []*Column) (string, error) {
	sortedColumns := make([]*Column, 0, len(columns))
	copy(sortedColumns, columns)

	sort.Slice(sortedColumns, func(i, j int) bool {
		return sortedColumns[i].Name < sortedColumns[j].Name
	})

	marshalled, err := json.Marshal(sortedColumns)
	if err != nil {
		return "", fmt.Errorf("json.Marshal: %w", err)
	}

	hashBytes := sha256.Sum256(marshalled)
	return hex.EncodeToString(hashBytes[:]), nil
}

func getSchemaFilePathTemplate(schemasDirPath string) string {
	return fmt.Sprintf("%s%s%s", schemasDirPath, "%s", consts.JsonExtension) 
}
