package table

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/artem-vildanov/small-db/internal/consts"
	"github.com/artem-vildanov/small-db/internal/page"
	"github.com/artem-vildanov/small-db/internal/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_InitTableManager(t *testing.T) {
	var (
		tableDirPath = "./"

		tableName1 = "table_1"
		tableName2 = "table_2"
		tableName3 = "table_3"

		metadataFileNameWithSuffix1 = tableName1 + consts.JsonExtension
		metadataFileNameWithSuffix2 = tableName2 + consts.JsonExtension
		metadataFileNameWithSuffix3 = tableName3 + consts.JsonExtension

		dataFileNameWithSuffix1 = tableName1 + consts.DataExtension
		dataFileNameWithSuffix2 = tableName2 + consts.DataExtension
		dataFileNameWithSuffix3 = tableName3 + consts.DataExtension

		metadataPath1 = tableDirPath + metadataFileNameWithSuffix1
		metadataPath2 = tableDirPath + metadataFileNameWithSuffix2
		metadataPath3 = tableDirPath + metadataFileNameWithSuffix3

		dataPath1 = tableDirPath + dataFileNameWithSuffix1
		dataPath2 = tableDirPath + dataFileNameWithSuffix2
		dataPath3 = tableDirPath + dataFileNameWithSuffix3

		schemaID1  = "qwerqwer"
		schemaID2  = "zxvxcvzxzvc"
		schemaID3  = "asdfasdfasfas"
		schemaHash = "asdfasfasdfasdf"
	)

	createdAt, err := time.Parse("2006-01-02T15:04:05Z", "2012-01-02T15:04:05Z")
	require.NoError(t, err)

	getColumns := func(schemaNum int) []*schema.Column {
		return []*schema.Column{
			{
				Name:     fmt.Sprintf("schema%d_col1", schemaNum),
				Type:     schema.StringType,
				Size:     schema.DynamicMemoTypeColumnSize,
				Nullable: false,
			},
			{
				Name:     fmt.Sprintf("schema%d_col2", schemaNum),
				Type:     schema.Int32Type,
				Size:     int(schema.Int32Size),
				Nullable: false,
			},
			{
				Name:     fmt.Sprintf("schema%d_col3", schemaNum),
				Type:     schema.BoolType,
				Size:     int(schema.BoolSize),
				Nullable: false,
			},
		}
	}

	schemaManager := &schema.SchemaManager{
		IdToSchema: map[string]*schema.Schema{
			schemaID1: {
				ID:      schemaID1,
				Hash:    schemaHash,
				Columns: getColumns(1),
			},
			schemaID2: {
				ID:      schemaID2,
				Hash:    schemaHash,
				Columns: getColumns(2),
			},
			schemaID3: {
				ID:      schemaID3,
				Hash:    schemaHash,
				Columns: getColumns(3),
			},
		},
	}

	tableMeta1Json := `
	{
		"schemaId": "qwerqwer",
		"numPages": 12,
		"createdAt": "2012-01-02T15:04:05Z"
	}
	`

	tableMeta2Json := `
	{
		"schemaId": "zxvxcvzxzvc",
		"numPages": 12,
		"createdAt": "2012-01-02T15:04:05Z"
	}
	`

	tableMeta3Json := `
	{
		"schemaId": "asdfasdfasfas",
		"numPages": 1,
		"createdAt": "2012-01-02T15:04:05Z"
	}
	`

	table1 := &Table{
		Path:      dataPath1,
		Name:      tableName1,
		NumPages:  12,
		Schema:    schemaManager.IdToSchema[schemaID1],
		CreatedAt: createdAt,
	}

	table2 := &Table{
		Path:      dataPath2,
		Name:      tableName2,
		NumPages:  12,
		Schema:    schemaManager.IdToSchema[schemaID2],
		CreatedAt: createdAt,
	}

	table3 := &Table{
		Path:      dataPath3,
		Name:      tableName3,
		NumPages:  1,
		Schema:    schemaManager.IdToSchema[schemaID3],
		CreatedAt: createdAt,
	}

	require.NoError(t, os.WriteFile(
		metadataFileNameWithSuffix1,
		[]byte(tableMeta1Json),
		consts.PosixAccessRight,
	))
	require.NoError(t, os.WriteFile(
		metadataFileNameWithSuffix2,
		[]byte(tableMeta2Json),
		consts.PosixAccessRight,
	))
	require.NoError(t, os.WriteFile(
		metadataFileNameWithSuffix3,
		[]byte(tableMeta3Json),
		consts.PosixAccessRight,
	))

	defer func() {
		require.NoError(t, os.Remove(metadataPath1))
		require.NoError(t, os.Remove(metadataPath2))
		require.NoError(t, os.Remove(metadataPath3))
	}()

	tableManager, err := InitTableManager(tableDirPath, schemaManager)
	require.NoError(t, err)

	assert.Equal(t, tableDirPath, tableManager.tableDirPath)
	assert.Equal(t, 3, len(tableManager.NameToTable))
	assert.Equal(t, table1, tableManager.NameToTable[tableName1])
	assert.Equal(t, table2, tableManager.NameToTable[tableName2])
	assert.Equal(t, table3, tableManager.NameToTable[tableName3])
}

func TestTableManager_CreateNewTable(t *testing.T) {
	var (
		tableDirPath = "./"
		tableName    = "new_table"
		metadataPath = "./new_table.json"
		dataPath     = "./new_table.data"
	)

	schema := &schema.Schema{
		ID:   "schema_id_1",
		Hash: "hashhash",
		Columns: []*schema.Column{
			{
				Name:     fmt.Sprintf("schema%d_col1", 1),
				Type:     schema.StringType,
				Size:     schema.DynamicMemoTypeColumnSize,
				Nullable: false,
			},
			{
				Name:     fmt.Sprintf("schema%d_col2", 1),
				Type:     schema.Int32Type,
				Size:     int(schema.Int32Size),
				Nullable: false,
			},
			{
				Name:     fmt.Sprintf("schema%d_col3", 1),
				Type:     schema.BoolType,
				Size:     int(schema.BoolSize),
				Nullable: false,
			},
		},
	}

	tableManager := &TableManager{
		tableDirPath: tableDirPath,
		NameToTable:  make(map[string]*Table, 0),
	}

	expectedTable := &Table{
		Path:     tableManager.getDataFilePath(tableName),
		Name:     tableName,
		NumPages: 0,
		Schema:   schema,
	}

	gotTable, err := tableManager.CreateNewTable(tableName, schema)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, os.Remove(metadataPath))
		require.NoError(t, os.Remove(dataPath))
	}()

	expectedTable.CreatedAt = gotTable.CreatedAt

	assert.Equal(t, expectedTable, gotTable)
	assert.Equal(t, expectedTable, tableManager.NameToTable[tableName])

	metadataFileInfo, err := os.Stat(metadataPath)
	require.NoError(t, err)
	dataFileInfo, err := os.Stat(dataPath)
	require.NoError(t, err)

	assert.Equal(t, int64(0), dataFileInfo.Size())
	assert.NotEqual(t, int64(0), metadataFileInfo.Size())

	metadataMarshalled, err := os.ReadFile(metadataPath)
	require.NoError(t, err)

	var metadata TableMetadata
	require.NoError(t, json.Unmarshal(metadataMarshalled, &metadata))

	assert.Equal(t, metadata.SchemaID, expectedTable.Schema.ID)
	assert.Equal(t, metadata.NumPages, expectedTable.NumPages)
	assert.Equal(
		t,
		expectedTable.CreatedAt.Truncate(time.Minute),
		metadata.CreatedAt.Truncate(time.Minute),
	)
}

func TestTableManager_Insert(t *testing.T) {
	var (
		tableDirPath = "./"
		tableName    = "new_table"
	)

	tableSchema := &schema.Schema{
		ID:   "schema_id_1",
		Hash: "hashhash",
		Columns: []*schema.Column{
			{
				Name:     "schema1_col1",
				Type:     schema.StringType,
				Size:     schema.DynamicMemoTypeColumnSize,
				Nullable: false,
			},
			{
				Name:     "schema1_col2",
				Type:     schema.Int32Type,
				Size:     int(schema.Int32Size),
				Nullable: false,
			},
			{
				Name:     "schema1_col3",
				Type:     schema.BoolType,
				Size:     int(schema.BoolSize),
				Nullable: false,
			},
		},
		NameToColumn: map[string]*schema.Column{
			"schema1_col1": {
				Name:     "schema1_col1",
				Type:     schema.StringType,
				Size:     schema.DynamicMemoTypeColumnSize,
				Nullable: false,
			},
			"schema1_col2": {
				Name:     "schema1_col2",
				Type:     schema.Int32Type,
				Size:     int(schema.Int32Size),
				Nullable: false,
			},
			"schema1_col3": {
				Name:     "schema1_col3",
				Type:     schema.BoolType,
				Size:     int(schema.BoolSize),
				Nullable: false,
			},
		},
	}

	schemaManager := &schema.SchemaManager{
		IdToSchema: map[string]*schema.Schema{
			tableSchema.ID: tableSchema,
		},
	}

	testCases := []struct {
		name           string
		inputRecord    map[string]any
		expectedRecord *Record
		errMessage     *string
	}{
		{
			name: "успешная вставка",
			inputRecord: map[string]any{
				"schema1_col1": "hello world",
				"schema1_col2": 199,
				"schema1_col3": true,
			},
			expectedRecord: &Record{
				Fields: []*Field{
					{
						Column: &schema.Column{
							Name:     "schema1_col1",
							Type:     schema.StringType,
							Size:     schema.DynamicMemoTypeColumnSize,
							Nullable: false,
						},
						Value: []byte{104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100},
					},
					{
						Column: &schema.Column{
							Name:     "schema1_col2",
							Type:     schema.Int32Type,
							Size:     int(schema.Int32Size),
							Nullable: false,
						},
						Value: []byte{0, 0, 0, 199},
					},
					{
						Column: &schema.Column{
							Name:     "schema1_col3",
							Type:     schema.BoolType,
							Size:     int(schema.BoolSize),
							Nullable: false,
						},
						Value: []byte{1},
					},
				},
			},
		},
		{
			name: "несуществующая колонка",
			inputRecord: map[string]any{
				"schema1_col1": "hello world",
				"schema1_col2": 199,
				"schema1_col4": true,
			},
			errMessage: pointer.To("NewRecordInSchema: no column with name schema1_col4 in schema"),
		},
		{
			name: "неподходящий	тип",
			inputRecord: map[string]any{
				"schema1_col1": "hello world",
				"schema1_col2": 199,
				"schema1_col3": 8989,
			},
			errMessage: pointer.To("NewRecordInSchema: serializeValue: failed to serialize bool value"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				dataFilePath     = tableDirPath + tableName + consts.DataExtension
				metadataFilePath = tableDirPath + tableName + consts.JsonExtension
				assertRecords    = getRecordsAsserter(t)
			)

			tableManager, err := InitTableManager(tableDirPath, schemaManager)
			require.NoError(t, err)

			_, err = tableManager.CreateNewTable(tableName, tableSchema)
			require.NoError(t, err)

			defer func() {
				require.NoError(t, os.Remove(dataFilePath))
				require.NoError(t, os.Remove(metadataFilePath))
			}()

			err = tableManager.Insert(tableName, tc.inputRecord)
			if tc.errMessage != nil {
				assert.Equal(t, *tc.errMessage, err.Error())

				info, err := os.Stat(dataFilePath)
				require.NoError(t, err)

				assert.Equal(t, int64(0), info.Size())

				return
			}

			require.NoError(t, err)

			// сравниваем с инициализированным tableManager
			// то есть с значениями в памяти

			gotRecords, err := tableManager.GetAllRecords(tableName)
			require.NoError(t, err)
			require.Equal(t, 1, len(gotRecords))

			assertRecords(gotRecords[0], tc.expectedRecord)

			// сравниваем с переинициализированным tableManager
			// то есть с значениями из диска

			tableManager, err = InitTableManager(tableDirPath, schemaManager)
			require.NoError(t, err)

			gotRecords, err = tableManager.GetAllRecords(tableName)

			require.NoError(t, err)
			require.Equal(t, 1, len(gotRecords))

			assertRecords(gotRecords[0], tc.expectedRecord)
		})
	}
}

func TestTableManager_FindByCondition(t *testing.T) {
	var (
		assertRecords = getRecordsAsserter(t)
		tableName,
		tableManager,
		record1,
		record2,
		record3,
		clear = initTableWithStaticRecords(t)
	)

	defer clear(t)

	testCases := []struct {
		name            string
		condition       func(map[string]any) bool
		expectedRecords []*Record
	}{
		{
			name: "нашел одну запись",
			condition: func(r map[string]any) bool {
				return r["schema1_col2"].(int32) == 123
			},
			expectedRecords: []*Record{record1},
		},
		{
			name: "нашел две записи по отрицательному числу",
			condition: func(r map[string]any) bool {
				return r["schema1_col2"].(int32) == -123
			},
			expectedRecords: []*Record{record2, record3},
		},
		{
			name: "нашел все записи",
			condition: func(r map[string]any) bool {
				return r["schema1_col1"].(string) == "something interesting" ||
					r["schema1_col1"].(string) == "qwe"
			},
			expectedRecords: []*Record{record1, record2, record3},
		},
		{
			name: "ничего не найдено",
			condition: func(r map[string]any) bool {
				return r["schema1_col1"].(string) == "zxc"
			},
			expectedRecords: make([]*Record, 0),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotRecords, err := tableManager.FindByCondition(tableName, tc.condition)
			require.NoError(t, err)
			require.Equal(t, len(tc.expectedRecords), len(gotRecords))

			for i := 0; i < len(gotRecords); i++ {
				expectedRecord := tc.expectedRecords[i]
				gotRecord := gotRecords[i]
				assertRecords(expectedRecord, gotRecord)
			}
		})
	}
}

func TestTableManager_UpdateByCondition(t *testing.T) {
	var (
		tableName,
		tableManager,
		_,
		_,
		_,
		clear = initTableWithStaticRecords(t)
	)

	defer clear(t)

	t.Run("успешное обновление некоторых записей", func(t *testing.T) {
		require.NoError(t, tableManager.UpdateByCondition(
			tableName,
			func(r map[string]any) bool {
				return r["schema1_col2"].(int32) == -123
			},
			func(r map[string]any) {
				r["schema1_col2"] = 321
				r["schema1_col1"] = "updated value"
			},
		))

		foundRecords, err := tableManager.FindByCondition(tableName, func(r map[string]any) bool {
			return r["schema1_col2"].(int32) == -123
		})
		require.NoError(t, err)
		assert.Equal(t, 0, len(foundRecords))

		foundRecords, err = tableManager.FindByCondition(tableName, func(r map[string]any) bool {
			return r["schema1_col2"].(int32) == 321
		})
		require.NoError(t, err)
		require.Equal(t, 2, len(foundRecords))

		for _, foundRecord := range foundRecords {
			intVal, err := foundRecord.GetInt32FieldValue("schema1_col2")
			require.NoError(t, err)

			strVal, err := foundRecord.GetStringFieldValue("schema1_col1")
			require.NoError(t, err)

			assert.Equal(t, 321, int(intVal))
			assert.Equal(t, "updated value", strVal)
		}

		foundRecords, err = tableManager.FindByCondition(tableName, func(r map[string]any) bool {
			return r["schema1_col2"].(int32) == 123
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(foundRecords))

		intVal, err := foundRecords[0].GetInt32FieldValue("schema1_col2")
		require.NoError(t, err)

		strVal, err := foundRecords[0].GetStringFieldValue("schema1_col1")
		require.NoError(t, err)

		assert.Equal(t, int32(123), intVal)
		assert.Equal(t, "something interesting", strVal)
	})
}

func TestTableManager_FullVacuum(t *testing.T) {
	const (
		columnName1                 = "schema1_col1"
		recordsNum                  = 7
		recordsStringValueLen       = 2000
		recordsIntValueMax    int64 = 100
	)

	var (
		tableName,
		tableManager,
		records,
		clean = initTableWithRandRecords(
			t,
			recordsNum,
			recordsStringValueLen,
			recordsIntValueMax,
		)
	)
	defer clean()

	recordForDeletion1 := records[0]
	recForDeleteStringVal1, err := recordForDeletion1.GetStringFieldValue(columnName1)
	require.NoError(t, err)

	recordForDeletion2 := records[2]
	recForDeleteStringVal2, err := recordForDeletion2.GetStringFieldValue(columnName1)
	require.NoError(t, err)

	recordForDeletion3 := records[3]
	recForDeleteStringVal3, err := recordForDeletion3.GetStringFieldValue(columnName1)
	require.NoError(t, err)

	recordForDeletion4 := records[5]
	recForDeleteStringVal4, err := recordForDeletion4.GetStringFieldValue(columnName1)
	require.NoError(t, err)

	require.NoError(t, tableManager.DeleteByCondition(
		tableName, func(record map[string]any) bool {
			v := record[columnName1]
			return v == recForDeleteStringVal1 ||
				v == recForDeleteStringVal2 ||
				v == recForDeleteStringVal3 ||
				v == recForDeleteStringVal4
		}),
	)

	expectActiveRecord1 := records[1]
	activeRecStringVal1, err := expectActiveRecord1.GetStringFieldValue(columnName1)
	require.NoError(t, err)

	expectActiveRecord2 := records[4]
	activeRecStringVal2, err := expectActiveRecord2.GetStringFieldValue(columnName1)
	require.NoError(t, err)

	expectActiveRecord3 := records[6]
	activeRecStringVal3, err := expectActiveRecord3.GetStringFieldValue(columnName1)
	require.NoError(t, err)

	require.NoError(t, tableManager.FullVacuum(tableName))

	table := tableManager.NameToTable[tableName]
	dataDescriptor, err := tableManager.openFile(table.Path)
	require.NoError(t, err)

	iter, err := page.NewPagesIter(dataDescriptor)
	require.NoError(t, err)

	gotRecords := make([]*Record, 0)
	for iter.Next() {
		tablePage, err := iter.GetPage()
		require.NoError(t, err)

		for _, ptr := range tablePage.Pointers {
			data := tablePage.GetDataByPointer(ptr)
			gotRecords = append(gotRecords, DeserializeRecordBySchema(table.Schema, data))
		}
	}

	require.Equal(t, 3, len(gotRecords))

	var (
		gotActiveRecord1,
		gotActiveRecord2,
		gotActiveRecord3 *Record
	)

	for _, gotRec := range gotRecords {
		gotRecStrVal, err := gotRec.GetStringFieldValue(columnName1)
		require.NoError(t, err)

		switch gotRecStrVal {
		case activeRecStringVal1:
			gotActiveRecord1 = gotRec
		case activeRecStringVal2:
			gotActiveRecord2 = gotRec
		case activeRecStringVal3:
			gotActiveRecord3 = gotRec
		default:
			t.Fatalf("got unexpected string value in column %s", columnName1)
		}
	}

	assert.Equal(t, expectActiveRecord1, gotActiveRecord1)
	assert.Equal(t, expectActiveRecord2, gotActiveRecord2)
	assert.Equal(t, expectActiveRecord3, gotActiveRecord3)
}

func TestTableManager_Vacuum(t *testing.T) {
	// todo: implement
}

func TestTableManager_ShouldVacuum(t *testing.T) {
	// todo: implement
}

func getTestSerializer(t *testing.T) func(schema.ColumnType, any) []byte {
	return func(columnType schema.ColumnType, r any) []byte {
		v, err := serializeValue(columnType, r)
		require.NoError(t, err)
		return v
	}
}

func getRecordsAsserter(t *testing.T) func(rec1 *Record, rec2 *Record) {
	return func(rec1 *Record, rec2 *Record) {
		sort.Slice(rec1.Fields, func(i, j int) bool {
			return rec1.Fields[i].Column.Name >
				rec1.Fields[j].Column.Name
		})

		sort.Slice(rec2.Fields, func(i, j int) bool {
			return rec2.Fields[i].Column.Name >
				rec2.Fields[j].Column.Name
		})

		assert.Equal(t, rec1.Fields, rec2.Fields)
	}
}

func initTableWithStaticRecords(t *testing.T) (
	tableName string,
	tableManager *TableManager,
	record1 *Record,
	record2 *Record,
	record3 *Record,
	clear func(t *testing.T),
) {
	tableName = "new_table"

	var (
		tableDirPath     = "./"
		dataFilePath     = tableDirPath + tableName + consts.DataExtension
		metadataFilePath = tableDirPath + tableName + consts.JsonExtension
		serialize        = getTestSerializer(t)
	)

	tableSchema := &schema.Schema{
		ID:   "schema_id_1",
		Hash: "hashhash",
		Columns: []*schema.Column{
			{
				Name:     "schema1_col1",
				Type:     schema.StringType,
				Size:     schema.DynamicMemoTypeColumnSize,
				Nullable: false,
			},
			{
				Name:     "schema1_col2",
				Type:     schema.Int32Type,
				Size:     int(schema.Int32Size),
				Nullable: false,
			},
			{
				Name:     "schema1_col3",
				Type:     schema.BoolType,
				Size:     int(schema.BoolSize),
				Nullable: false,
			},
		},
		NameToColumn: map[string]*schema.Column{
			"schema1_col1": {
				Name:     "schema1_col1",
				Type:     schema.StringType,
				Size:     schema.DynamicMemoTypeColumnSize,
				Nullable: false,
			},
			"schema1_col2": {
				Name:     "schema1_col2",
				Type:     schema.Int32Type,
				Size:     int(schema.Int32Size),
				Nullable: false,
			},
			"schema1_col3": {
				Name:     "schema1_col3",
				Type:     schema.BoolType,
				Size:     int(schema.BoolSize),
				Nullable: false,
			},
		},
	}

	record1 = &Record{
		Fields: []*Field{
			{
				Column: &schema.Column{
					Name:     "schema1_col1",
					Type:     schema.StringType,
					Size:     schema.DynamicMemoTypeColumnSize,
					Nullable: false,
				},
				Value: serialize(schema.StringType, "something interesting"),
			},
			{
				Column: &schema.Column{
					Name:     "schema1_col2",
					Type:     schema.Int32Type,
					Size:     int(schema.Int32Size),
					Nullable: false,
				},
				Value: serialize(schema.Int32Type, 123),
			},
			{
				Column: &schema.Column{
					Name:     "schema1_col3",
					Type:     schema.BoolType,
					Size:     int(schema.BoolSize),
					Nullable: false,
				},
				Value: serialize(schema.BoolType, false),
			},
		},
	}

	record2 = &Record{
		Fields: []*Field{
			{
				Column: &schema.Column{
					Name:     "schema1_col1",
					Type:     schema.StringType,
					Size:     schema.DynamicMemoTypeColumnSize,
					Nullable: false,
				},
				Value: serialize(schema.StringType, "something interesting"),
			},
			{
				Column: &schema.Column{
					Name:     "schema1_col2",
					Type:     schema.Int32Type,
					Size:     int(schema.Int32Size),
					Nullable: false,
				},
				Value: serialize(schema.Int32Type, -123),
			},
			{
				Column: &schema.Column{
					Name:     "schema1_col3",
					Type:     schema.BoolType,
					Size:     int(schema.BoolSize),
					Nullable: false,
				},
				Value: serialize(schema.BoolType, false),
			},
		},
	}

	record3 = &Record{
		Fields: []*Field{
			{
				Column: &schema.Column{
					Name:     "schema1_col1",
					Type:     schema.StringType,
					Size:     schema.DynamicMemoTypeColumnSize,
					Nullable: false,
				},
				Value: serialize(schema.StringType, "qwe"),
			},
			{
				Column: &schema.Column{
					Name:     "schema1_col2",
					Type:     schema.Int32Type,
					Size:     int(schema.Int32Size),
					Nullable: false,
				},
				Value: serialize(schema.Int32Type, -123),
			},
			{
				Column: &schema.Column{
					Name:     "schema1_col3",
					Type:     schema.BoolType,
					Size:     int(schema.BoolSize),
					Nullable: false,
				},
				Value: serialize(schema.BoolType, true),
			},
		},
	}
	schemaManager := &schema.SchemaManager{
		IdToSchema: map[string]*schema.Schema{
			tableSchema.ID: tableSchema,
		},
	}

	tableManager, err := InitTableManager(tableDirPath, schemaManager)
	require.NoError(t, err)

	_, err = tableManager.CreateNewTable(tableName, tableSchema)
	require.NoError(t, err)

	clear = func(t *testing.T) {
		require.NoError(t, os.Remove(dataFilePath))
		require.NoError(t, os.Remove(metadataFilePath))
	}

	err = tableManager.Insert(tableName, map[string]any{
		"schema1_col1": "something interesting",
		"schema1_col2": 123,
		"schema1_col3": false,
	})
	require.NoError(t, err)

	err = tableManager.Insert(tableName, map[string]any{
		"schema1_col1": "something interesting",
		"schema1_col2": -123,
		"schema1_col3": false,
	})
	require.NoError(t, err)

	err = tableManager.Insert(tableName, map[string]any{
		"schema1_col1": "qwe",
		"schema1_col2": -123,
		"schema1_col3": true,
	})
	require.NoError(t, err)

	return
}

func initTableWithRandRecords(
	t *testing.T,
	numRecords int,
	recordsStringValueLen int,
	recordsIntValueMax int64,
) (
	tableName string,
	tableManager *TableManager,
	records []*Record,
	clear func(),
) {
	tableName = "new_table"

	var (
		tableDirPath     = "./"
		dataFilePath     = tableDirPath + tableName + consts.DataExtension
		metadataFilePath = tableDirPath + tableName + consts.JsonExtension
		serialize        = getTestSerializer(t)
	)

	const (
		schemaID    = "schema_id_1"
		columnName1 = "schema1_col1"
		columnName2 = "schema1_col2"
		columnName3 = "schema1_col3"
	)

	tableSchema := &schema.Schema{
		ID:   schemaID,
		Hash: "hashhash",
		Columns: []*schema.Column{
			{
				Name:     columnName1,
				Type:     schema.StringType,
				Size:     schema.DynamicMemoTypeColumnSize,
				Nullable: false,
			},
			{
				Name:     columnName2,
				Type:     schema.Int32Type,
				Size:     int(schema.Int32Size),
				Nullable: false,
			},
			{
				Name:     columnName3,
				Type:     schema.BoolType,
				Size:     int(schema.BoolSize),
				Nullable: false,
			},
		},
		NameToColumn: map[string]*schema.Column{
			columnName1: {
				Name:     columnName1,
				Type:     schema.StringType,
				Size:     schema.DynamicMemoTypeColumnSize,
				Nullable: false,
			},
			columnName2: {
				Name:     columnName2,
				Type:     schema.Int32Type,
				Size:     int(schema.Int32Size),
				Nullable: false,
			},
			columnName3: {
				Name:     columnName3,
				Type:     schema.BoolType,
				Size:     int(schema.BoolSize),
				Nullable: false,
			},
		},
	}

	for i := 0; i < numRecords; i++ {
		stringBuff := make([]byte, recordsStringValueLen/2+(recordsStringValueLen%2))
		_, err := rand.Read(stringBuff)
		require.NoError(t, err)
		randString := hex.EncodeToString(stringBuff)

		randInt, err := rand.Int(rand.Reader, big.NewInt(recordsIntValueMax))
		require.NoError(t, err)

		randBool, err := rand.Int(rand.Reader, big.NewInt(1))
		require.NoError(t, err)

		record := NewEmptyRecord()
		record.addFields(
			&Field{
				Column: &schema.Column{
					Name:     columnName1,
					Type:     schema.StringType,
					Size:     schema.DynamicMemoTypeColumnSize,
					Nullable: false,
				},
				Value: serialize(schema.StringType, randString[:recordsStringValueLen]),
			},
			&Field{
				Column: &schema.Column{
					Name:     columnName2,
					Type:     schema.Int32Type,
					Size:     int(schema.Int32Size),
					Nullable: false,
				},
				Value: serialize(schema.Int32Type, int32(randInt.Int64())),
			},
			&Field{
				Column: &schema.Column{
					Name:     columnName3,
					Type:     schema.BoolType,
					Size:     int(schema.BoolSize),
					Nullable: false,
				},
				Value: serialize(schema.BoolType, randBool.Int64() == 1),
			},
		)

		records = append(records, record)
	}

	schemaManager := &schema.SchemaManager{
		IdToSchema: map[string]*schema.Schema{
			tableSchema.ID: tableSchema,
		},
	}

	tableManager, err := InitTableManager(tableDirPath, schemaManager)
	require.NoError(t, err)

	_, err = tableManager.CreateNewTable(tableName, tableSchema)
	require.NoError(t, err)

	clear = func() {
		require.NoError(t, os.Remove(dataFilePath))
		require.NoError(t, os.Remove(metadataFilePath))
	}

	for _, record := range records {
		columnValue1, err := record.GetStringFieldValue(columnName1)
		require.NoError(t, err)

		columnValue2, err := record.GetInt32FieldValue(columnName2)
		require.NoError(t, err)

		columnValue3, err := record.GetBoolFieldValue(columnName3)
		require.NoError(t, err)

		require.NoError(t, tableManager.Insert(tableName, map[string]any{
			columnName1: columnValue1,
			columnName2: columnValue2,
			columnName3: columnValue3,
		}))
	}

	return
}
