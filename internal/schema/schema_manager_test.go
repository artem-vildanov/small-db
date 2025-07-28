package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_InitSchemaManager(t *testing.T) {
	var (
		schemaID1 = "qwreqwer"
		schemaID2 = "asdfasdf"
		schemaID3 = "zxcvzxcv"

		schemasDirPath          = "./"
		schemasFilePathTemplate = getSchemaFilePathTemplate(schemasDirPath)

		schema1FilePath = fmt.Sprintf(schemasFilePathTemplate, schemaID1)
		schema2FilePath = fmt.Sprintf(schemasFilePathTemplate, schemaID2)
		schema3FilePath = fmt.Sprintf(schemasFilePathTemplate, schemaID3)
	)

	schema1Marshalled := `
	{
		"id": "qwreqwer",
		"hash": "asfasfasdfasfdasdfasfasf",
		"columns": [
			{
				"name": "first_string_col1",
				"type": "string",
				"size": -1,
				"nullable": false
			},
			{
				"name": "second_int_col1",
				"type": "int32",
				"size": 4,
				"nullable": false
			}
		]
	}
	`

	schema2Marshalled := `
	{
		"id": "asdfasdf",
		"hash": "asfasfasdfasfdasdfasfasf",
		"columns": [
			{
				"name": "first_string_col2",
				"type": "string",
				"size": -1,
				"nullable": false
			},
			{
				"name": "second_int_col2",
				"type": "int32",
				"size": 4,
				"nullable": false
			},
			{
				"name": "third_bool_col2",
				"type": "bool",
				"size": 1,
				"nullable": false
			}
		]
	}
	`

	schema3Marshalled := `
	{
		"id": "zxcvzxcv",
		"hash": "asfasfasdfasfdasdfasfasf",
		"columns": [
			{
				"name": "second_int_col3",
				"type": "int32",
				"size": 4,
				"nullable": false
			},
			{
				"name": "third_bool_col3",
				"type": "bool",
				"size": 1,
				"nullable": false
			}
		]
	}
	`

	schema1Columns := []*Column{
		{
			Name:     "first_string_col1",
			Type:     StringType,
			Size:     -1,
			Nullable: false,
		},
		{
			Name:     "second_int_col1",
			Type:     Int32Type,
			Size:     int(Int32Size),
			Nullable: false,
		},
	}

	schema2Columns := []*Column{
		{
			Name:     "first_string_col2",
			Type:     StringType,
			Size:     -1,
			Nullable: false,
		},
		{
			Name:     "second_int_col2",
			Type:     Int32Type,
			Size:     int(Int32Size),
			Nullable: false,
		},
		{
			Name:     "third_bool_col2",
			Type:     BoolType,
			Size:     int(BoolSize),
			Nullable: false,
		},
	}

	schema3Columns := []*Column{
		{
			Name:     "second_int_col3",
			Type:     Int32Type,
			Size:     int(Int32Size),
			Nullable: false,
		},
		{
			Name:     "third_bool_col3",
			Type:     BoolType,
			Size:     int(BoolSize),
			Nullable: false,
		},
	}

	schema1File, err := os.Create(schema1FilePath)
	require.NoError(t, err)
	schema2File, err := os.Create(schema2FilePath)
	require.NoError(t, err)
	schema3File, err := os.Create(schema3FilePath)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, os.Remove(schema1FilePath))
		require.NoError(t, os.Remove(schema2FilePath))
		require.NoError(t, os.Remove(schema3FilePath))
	}()

	_, err = schema1File.Write([]byte(schema1Marshalled))
	require.NoError(t, err)
	_, err = schema2File.Write([]byte(schema2Marshalled))
	require.NoError(t, err)
	_, err = schema3File.Write([]byte(schema3Marshalled))
	require.NoError(t, err)

	schema1File.Close()
	schema2File.Close()
	schema3File.Close()

	t.Run("успешно получили схемы", func(t *testing.T) {
		manager, err := InitSchemaManager(schemasDirPath)
		require.NoError(t, err)

		gotSchema1, ok := manager.IdToSchema[schemaID1]
		require.Equal(t, true, ok)
		assert.Equal(t, gotSchema1.ID, schemaID1)
		assert.Equal(t, gotSchema1.Hash, "asfasfasdfasfdasdfasfasf")
		assert.Equal(t, gotSchema1.Columns, schema1Columns)

		gotSchema2, ok := manager.IdToSchema[schemaID2]
		require.Equal(t, true, ok)
		assert.Equal(t, gotSchema2.ID, schemaID2)
		assert.Equal(t, gotSchema2.Hash, "asfasfasdfasfdasdfasfasf")
		assert.Equal(t, gotSchema2.Columns, schema2Columns)

		gotSchema3, ok := manager.IdToSchema[schemaID3]
		require.Equal(t, true, ok)
		assert.Equal(t, gotSchema3.ID, schemaID3)
		assert.Equal(t, gotSchema3.Hash, "asfasfasdfasfdasdfasfasf")
		assert.Equal(t, gotSchema3.Columns, schema3Columns)
	})
}

func TestSchemaManager_CreateNewSchema(t *testing.T) {
	var (
		schemasDirPath = "./"
	)

	columns := []*Column{
		{
			Name:     "first_name",
			Type:     StringType,
			Size:     DynamicMemoTypeColumnSize,
			Nullable: false,
		},
		{
			Name:     "second_name",
			Type:     Int32Type,
			Size:     int(Int32Size),
			Nullable: false,
		},
		{
			Name:     "third_name",
			Type:     BoolType,
			Size:     int(BoolSize),
			Nullable: false,
		},
	}

	manager, err := InitSchemaManager(schemasDirPath)
	require.NoError(t, err)

	schema, err := manager.CreateNewSchema(columns)
	require.NoError(t, err)

	filePath := fmt.Sprintf(getSchemaFilePathTemplate(schemasDirPath), schema.ID)

	defer func() {
		require.NoError(t, os.Remove(filePath))
	}()

	assert.Equal(t, columns, schema.Columns)

	rawSchema, err := os.ReadFile(filePath)
	require.NoError(t, err)

	var gotSchema Schema
	err = json.Unmarshal(rawSchema, &gotSchema)
	require.NoError(t, err)

	assert.Equal(t, schema.ID, gotSchema.ID)
	assert.Equal(t, schema.Columns, gotSchema.Columns)
	assert.Equal(t, schema.Hash, gotSchema.Hash)
}
