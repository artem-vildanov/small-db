package table

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/artem-vildanov/small-db/internal/consts"
	"github.com/artem-vildanov/small-db/internal/page"
	"github.com/artem-vildanov/small-db/internal/schema"
)

const recordsPreallocSize = 10
const vacuumBloatTreshold = 0.3

type TableManager struct {
	tableDirPath string
	NameToTable  map[string]*Table
}

func InitTableManager(
	tableDirPath string,
	schemaManager *schema.SchemaManager,
) (*TableManager, error) {
	entries, err := os.ReadDir(tableDirPath)
	if err != nil {
		return nil, fmt.Errorf("os.ReadDir: %w", err)
	}

	tableManager := &TableManager{
		tableDirPath: tableDirPath,
		NameToTable:  make(map[string]*Table, len(entries)/2),
	}

	for _, entry := range entries {
		isFile := entry.Type().IsRegular()
		isMetadata := filepath.Ext(entry.Name()) == consts.JsonExtension

		if !isFile || !isMetadata {
			continue
		}

		tableName := strings.TrimSuffix(entry.Name(), consts.JsonExtension)
		metadataFilePath := fmt.Sprintf("%s%s", tableDirPath, entry.Name())
		dataFilePath := fmt.Sprintf("%s%s%s", tableDirPath, tableName, consts.DataExtension)

		marshalledMetadata, err := os.ReadFile(metadataFilePath)
		if err != nil {
			return nil, fmt.Errorf("os.ReadFile: %w", err)
		}

		var metadata TableMetadata
		if err := json.Unmarshal(marshalledMetadata, &metadata); err != nil {
			return nil, fmt.Errorf("json.Unmarshal: %w", err)
		}

		tableManager.NameToTable[tableName] = &Table{
			Path:      dataFilePath,
			Name:      tableName,
			NumPages:  metadata.NumPages,
			CreatedAt: metadata.CreatedAt,
			Schema:    schemaManager.IdToSchema[metadata.SchemaID],
		}
	}

	return tableManager, nil
}

func (m *TableManager) CreateNewTable(tableName string, schema *schema.Schema) (*Table, error) {
	if _, exists := m.NameToTable[tableName]; exists {
		return nil, ErrTableWithNameExists(tableName)
	}

	var (
		createdAt         = time.Now()
		dataPath     = m.getDataFilePath(tableName)
		metadataPath = m.getMetadataFilePath(tableName)
		table             = &Table{
			Path:      dataPath,
			Name:      tableName,
			NumPages:  0,
			Schema:    schema,
			CreatedAt: createdAt,
		}
	)

	// создаем пустой файл для данных
	dataFile, err := m.createIfNotExists(dataPath)
	if err != nil {
		return nil, fmt.Errorf("TableManager.createIfNotExists: %w", err)
	}
	defer dataFile.Close()

	m.NameToTable[tableName] = table

	tableMetadata := &TableMetadata{
		SchemaID:  schema.ID,
		NumPages:  0,
		CreatedAt: createdAt,
	}

	metadataFile, err := m.createIfNotExists(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("TableManager.createIfNotExists: %w", err)
	}
	defer metadataFile.Close()

	metadataMarshalled, err := json.Marshal(tableMetadata)
	if err != nil {
		return nil, fmt.Errorf("json.Marshal: %w", err)
	}

	if _, err := metadataFile.Write(metadataMarshalled); err != nil {
		return nil, fmt.Errorf("File.Write: %w", err)
	}

	return table, nil
}

func (m *TableManager) Insert(tableName string, rawRecord map[string]any) error {
	table, exists := m.NameToTable[tableName]
	if !exists {
		return ErrTableWithNameDoesntExist(tableName)
	}

	dataDescriptor, err := m.openFile(table.Path)
	if err != nil {
		return fmt.Errorf("TableManager.openFile: %w", err)
	}
	defer dataDescriptor.Close()

	record, err := NewRecordInSchema(table.Schema, rawRecord)
	if err != nil {
		return fmt.Errorf("NewRecordInSchema: %w", err)
	}

	if err := m.checkUniqueConstraintViolation(table, record); err != nil {
		return fmt.Errorf("TableManager.checkUniqueConstraintViolation: %w", err)
	}

	if err := m.insertRecord(
		table,
		dataDescriptor,
		record,
	); err != nil {
		return fmt.Errorf("insertRecord: %w", err)
	}

	return nil
}

func (m *TableManager) checkUniqueConstraintViolation(table *Table, record *Record) error {
	pkToValue := make(map[string]any, len(table.Schema.PrimaryKeys))
	for _, pk := range table.Schema.PrimaryKeys {
		field := record.ColumnNameToField[pk]
		value, err := deserializeValue(field.Column.Type, field.Value)
		if err != nil {
			return fmt.Errorf("deserializeValue: %w", err)
		}

		pkToValue[pk] = value
	}

	violatedFields := make([]string, 0)
	notUniqueRecs, err := m.FindByCondition(
		table.Name,
		func(r map[string]any) bool {
			violationsCnt := 0

			for _, pk := range table.Schema.PrimaryKeys {
				if r[pk] != pkToValue[pk] {
					if len(violatedFields) == violationsCnt {
						violatedFields = append(violatedFields, pk)
					} 

					violationsCnt++
				}
			}

			return violationsCnt != 0
		},
	)
	if err != nil {
		return fmt.Errorf("TableManager.FindByCondition: %w", err)
	}

	if len(notUniqueRecs) != 0 {
		return ErrUniqueConstraintViolation(violatedFields)
	}

	return nil
}

func (m *TableManager) insertRecord(
	table *Table,
	dataDescriptor *os.File,
	record *Record,
) error {
	serializedRecord := record.Serialize()

	dataFileInfo, err := dataDescriptor.Stat()
	if err != nil {
		return fmt.Errorf("File.Stat: %w", err)
	}

	tableIsEmpty := (dataFileInfo.Size() / page.PageSize) == 0
	if tableIsEmpty {
		page := page.NewEmptyPage()

		if err := page.Insert(serializedRecord); err != nil {
			return fmt.Errorf("Page.Insert: %w", err)
		}

		serializedPage := page.Serialize()

		if _, err := dataDescriptor.Write(serializedPage); err != nil {
			return fmt.Errorf("File.Write: %w", err)
		}

		table.NumPages++
		if err := m.atomicUpdateMetadata(table); err != nil {
			return fmt.Errorf("TableManager.atomicUpdateMetadata: %w", err)
		}

		return nil
	} else {
		iterator, err := page.NewPagesIter(dataDescriptor)
		if err != nil {
			return fmt.Errorf("NewPagesIter: %w", err)
		}

		for iterator.Next() {
			tablePage, err := iterator.GetPage()
			if err != nil {
				return fmt.Errorf("pagesIterator.GetPage: %w", err)
			}

			if !tablePage.FreeSpaceMoreThanRequired(len(serializedRecord)) {
				continue
			}

			if err := tablePage.Insert(serializedRecord); err != nil {
				return fmt.Errorf("Page.Insert: %w", err)
			}

			if _, err := dataDescriptor.WriteAt(
				tablePage.Serialize(),
				iterator.GetPageOffset(),
			); err != nil {
				return fmt.Errorf("os.File.WriteAt: %w", err)
			}

			break
		}

		// todo: покрыть корнер кейс тестом
		if iterator.ReachedEnd() {
			// если обошли все существующие страницы и не нашли достаточно места,
			// то создаем новую страницу
			tablePage := page.NewEmptyPage()

			if err := tablePage.Insert(serializedRecord); err != nil {
				return fmt.Errorf("Page.Insert: %w", err)
			}

			newPageOffset := iterator.GetPageOffset() + page.PageSize
			if _, err := dataDescriptor.WriteAt(
				tablePage.Serialize(),
				newPageOffset,
			); err != nil {
				return fmt.Errorf("os.File.WriteAt: %w", err)
			}

			table.NumPages++
			if err := m.atomicUpdateMetadata(table); err != nil {
				return fmt.Errorf("TableManager.atomicUpdateMetadata: %w", err)
			}
		}
	}

	return nil
}

func (m *TableManager) atomicUpdateMetadata(table *Table) error {
	tableMetadata := &TableMetadata{
		SchemaID:  table.Schema.ID,
		NumPages:  table.NumPages,
		CreatedAt: table.CreatedAt,
	}

	metadataMarshalled, err := json.Marshal(tableMetadata)
	if err != nil {
		return fmt.Errorf("json.Marshal: %w", err)
	}

	descriptor, err := os.CreateTemp(m.tableDirPath, table.Name+".metadata.tmp")
	if err != nil {
		return fmt.Errorf("os.CreateTemp: %w", err)
	}
	defer descriptor.Close()

	if _, err := descriptor.Write(metadataMarshalled); err != nil {
		return fmt.Errorf("File.Write: %w", err)
	}

	if err := descriptor.Sync(); err != nil {
		return fmt.Errorf("File.Sync: %w", err)
	}

	tmpPath := descriptor.Name()
	metadataPath := m.getMetadataFilePath(table.Name)

	if err := os.Rename(
		tmpPath,
		metadataPath,
	); err != nil {
		return fmt.Errorf("os.Rename: %w", err)
	}

	return nil
}

func (m *TableManager) GetAllRecords(tableName string) ([]*Record, error) {
	result := make([]*Record, 0, recordsPreallocSize)
	if err := m.doByCondition(
		tableName,
		func(_ map[string]any) bool {
			return true
		},
		func(_ *os.File, _ *Table, matches []*matchedCondition) error {
			for _, matched := range matches {
				result = append(result, matched.Record)
			}
			return nil
		},
	); err != nil {
		return nil, fmt.Errorf("doByCondition: %w", err)
	}

	return result, nil
}

func (m *TableManager) FindByCondition(
	tableName string,
	match func(record map[string]any) bool,
) ([]*Record, error) {
	result := make([]*Record, 0, recordsPreallocSize)
	if err := m.doByCondition(
		tableName,
		match,
		func(_ *os.File, _ *Table, matches []*matchedCondition) error {
			for _, matched := range matches {
				result = append(result, matched.Record)
			}
			return nil
		},
	); err != nil {
		return nil, fmt.Errorf("doByCondition: %w", err)
	}

	return result, nil
}

func (m *TableManager) UpdateByCondition(
	tableName string,
	match func(record map[string]any) bool,
	update func(record map[string]any),
) error {
	metadataDescriptor, err := m.openFile(m.getMetadataFilePath(tableName))
	if err != nil {
		return fmt.Errorf("TableManager.openFile: %w", err)
	}
	defer metadataDescriptor.Close()

	callback := func(dataDescriptor *os.File, table *Table, matches []*matchedCondition) error {
		for _, matched := range matches {
			nameToValue, err := matched.Record.IntoNameToValue()
			if err != nil {
				return fmt.Errorf("Record.IntoNameToValue: %w", err)
			}

			update(nameToValue)

			updatedRecord, err := NewRecordInSchema(table.Schema, nameToValue)
			if err != nil {
				return fmt.Errorf("NewRecordInSchema: %w", err)
			}

			if err := m.insertRecord(
				table,
				dataDescriptor,
				updatedRecord,
			); err != nil {
				return fmt.Errorf("TableManager.insertRecord: %w", err)
			}

			if err := m.markRowAsDeleted(dataDescriptor, matched); err != nil {
				return fmt.Errorf("TableManager.markMatchedAsRemoved: %w", err)
			}
		}

		return nil
	}

	if err := m.doByCondition(
		tableName,
		match,
		callback,
	); err != nil {
		return fmt.Errorf("doByCondition: %w", err)
	}

	return nil
}

func (m *TableManager) DeleteByCondition(
	tableName string,
	match func(record map[string]any) bool,
) error {
	callback := func(dataDescriptor *os.File, table *Table, matches []*matchedCondition) error {
		for _, match := range matches {
			if err := m.markRowAsDeleted(dataDescriptor, match); err != nil {
				return fmt.Errorf("TableManager.markRowAsDeleted: %w", err)
			}
		}
		return nil
	}

	if err := m.doByCondition(tableName, match, callback); err != nil {
		return fmt.Errorf("TableManager.doByCondition: %w", err)
	}

	return nil
}

func (m *TableManager) markRowAsDeleted(descriptor *os.File, matched *matchedCondition) error {
	// устанавливаем статус deleted для указателя
	pointerOffset := matched.PageOffset +
		page.PageHeaderSize +
		int64(matched.PointerIndex)*page.ItemPointerSize

	// Offset(2) + Size(2)
	statusOffset := pointerOffset + 2 + 2

	if _, err := descriptor.WriteAt(
		[]byte{page.StatusDeleted},
		statusOffset,
	); err != nil {
		return fmt.Errorf("File.WriteAt: %w", err)
	}

	return nil
}

type matchedCondition struct {
	Record       *Record
	PointerIndex int
	PageOffset   int64
	Page         *page.Page
}

func (m *TableManager) doByCondition(
	tableName string,
	match func(record map[string]any) bool,
	do func(*os.File, *Table, []*matchedCondition) error,
) error {
	table, exists := m.NameToTable[tableName]
	if !exists {
		return ErrTableWithNameDoesntExist(tableName)
	}

	dataDescriptor, err := m.openFile(table.Path)
	if err != nil {
		return fmt.Errorf("TableManager.openFile: %w", err)
	}
	defer dataDescriptor.Close()

	iter, err := page.NewPagesIter(dataDescriptor)
	if err != nil {
		return fmt.Errorf("NewPagesIter: %w", err)
	}

	matches := make([]*matchedCondition, 0, recordsPreallocSize)

	for iter.Next() {
		tablePage, err := iter.GetPage()
		if err != nil {
			return fmt.Errorf("pagesIterator.GetPage: %w", err)
		}

		for pointerIndex, pointer := range tablePage.Pointers {
			// пропускаем блоат
			if pointer.Status != page.StatusActive {
				continue
			}

			data := tablePage.GetDataByPointer(pointer)
			record := DeserializeRecordBySchema(table.Schema, data)
			nameToValue, err := record.IntoNameToValue()
			if err != nil {
				return fmt.Errorf("Record.IntoNameToValue: %w", err)
			}

			if match(nameToValue) {
				matches = append(matches, &matchedCondition{
					Record:       record,
					PointerIndex: pointerIndex,
					PageOffset:   iter.GetPageOffset(),
					Page:         tablePage,
				})
			}
		}
	}

	if err := do(dataDescriptor, table, matches); err != nil {
		return fmt.Errorf("do: %w", err)
	}

	return nil
}

func (m *TableManager) ShouldVacuum(tableName string) (bool, error) {
	// todo: implement
	return false, errors.New("not implemented")
}

func (m *TableManager) FullVacuum(tableName string) error {
	table, exists := m.NameToTable[tableName]
	if !exists {
		return ErrTableWithNameDoesntExist(tableName)
	}

	dataDescriptor, err := m.openFile(table.Path)
	if err != nil {
		return fmt.Errorf("TableManager.openFile: %w", err)
	}
	defer dataDescriptor.Close()

	tmpDescriptor, err := os.CreateTemp(m.tableDirPath, tableName+".data.tmp")
	if err != nil {
		return fmt.Errorf("os.CreateTemp: %w", err)
	}
	defer tmpDescriptor.Close()

	iter, err := page.NewPagesIter(dataDescriptor)
	if err != nil {
		return fmt.Errorf("NewPagesIter: %w", err)
	}

	var (
		numPages         int
		bufferPage       = page.NewEmptyPage()
		bufferPageOffset int64
	)

	for iter.Next() {
		oldPage, err := iter.GetPage()
		if err != nil {
			return fmt.Errorf("pagesIterator.GetPage: %w", err)
		}

		for _, ptr := range oldPage.Pointers {
			if ptr.Status == page.StatusDeleted {
				continue
			}

			data := oldPage.GetDataByPointer(ptr)

			err := bufferPage.Insert(data)
			if err == nil {
				continue
			}

			var cantFitDataErr *page.ErrCantFitDataIntoPage
			if !errors.As(err, &cantFitDataErr) {
				return fmt.Errorf("Page.Insert: %w", err)
			}

			if _, err := tmpDescriptor.WriteAt(
				bufferPage.Serialize(),
				bufferPageOffset,
			); err != nil {
				return fmt.Errorf("File.WriteAt: %w", err)
			}

			bufferPage = page.NewEmptyPage()
			bufferPageOffset += page.PageSize
			numPages++

			if err := bufferPage.Insert(data); err != nil {
				return fmt.Errorf("Page.Insert: %w", err)
			}
		}
	}

	if len(bufferPage.Pointers) != 0 {
		if _, err := tmpDescriptor.WriteAt(
			bufferPage.Serialize(),
			bufferPageOffset,
		); err != nil {
			return fmt.Errorf("File.WriteAt: %w", err)
		}
	}

	if err := tmpDescriptor.Sync(); err != nil {
		return fmt.Errorf("File.Sync: %w", err)
	}

	table.NumPages = numPages
	if err := m.atomicUpdateMetadata(table); err != nil {
		return fmt.Errorf("TableManager.atomicUpdateMetadata: %w", err)
	}

	if err := os.Rename(
		tmpDescriptor.Name(),
		dataDescriptor.Name(),
	); err != nil {
		return fmt.Errorf("os.Rename: %w", err)
	}

	return nil
}

func (m *TableManager) ConcurrentVacuum(tableName string) error {
	// todo: impelement
	return errors.New("not implemented")
}

func (m *TableManager) openFile(filePath string) (*os.File, error) {
	file, err := os.OpenFile(
		filePath,
		os.O_RDWR,
		consts.PosixAccessRight,
	)
	if err != nil {
		return nil, fmt.Errorf("os.Open: %w", err)
	}

	return file, nil
}

func (m *TableManager) createIfNotExists(filePath string) (*os.File, error) {
	file, err := os.OpenFile(
		filePath,
		consts.CreateIfNotExists,
		consts.PosixAccessRight,
	)
	if err != nil {
		return nil, fmt.Errorf("os.Open: %w", err)
	}

	return file, nil
}

func (m *TableManager) getDataFilePath(fileName string) string {
	return fmt.Sprintf("%s%s%s", m.tableDirPath, fileName, consts.DataExtension)
}

func (m *TableManager) getMetadataFilePath(fileName string) string {
	return fmt.Sprintf("%s%s%s", m.tableDirPath, fileName, consts.JsonExtension)
}