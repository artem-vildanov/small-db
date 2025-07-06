package table

import (
	"fmt"
	"os"

	"github.com/artem-vildanov/small-db/internal/page"
)

const (
	// права доступа к файлу:
	// владелец может читать и писать
	// остальные только читать
	posixAccessRight = 0644

	// сочетание флагов:
	// создать файл, если не существует (O_CREATE)
	// ошибка, если файл уже существует (O_EXCL)
	// открыть только для записи (O_WRONLY)
	createIfNotExists = os.O_CREATE | os.O_EXCL | os.O_WRONLY
)

type Table struct {
	Path     string
	Name     string
	NumPages int
	Schema   *Schema
}

func CreateNewTable(path string, name string, schema *Schema) (*Table, error) {
	table := &Table{
		Path:     path,
		Name:     name,
		NumPages: 0,
		Schema:   schema,
	}

	_, err := os.OpenFile(path, createIfNotExists, posixAccessRight)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}

	return table, nil
}

func (t *Table) Insert(data []byte) error {
	fileInfo, err := os.Stat(t.Path)
	if err != nil {
		return fmt.Errorf("os.Stat: %w", err)
	}

	tableIsEmpty := (fileInfo.Size() / page.PageSize) == 0
	if tableIsEmpty {
		if err := t.insertFirstPage(data); err != nil {
			return fmt.Errorf("insertIntoEmptyPage: %w", err)
		}

		return nil
	}

	if err := t.insertIntoNonemptyTable(data); err != nil {
		return fmt.Errorf("insertIntoNonemptyPage: %w", err)
	}

	return nil
}

func (t *Table) FindByCondition(match func(*Record) bool) (*Record, error) {
	_, nextPage, err := t.pagesIterator()
	if err != nil {
		return nil, fmt.Errorf("Table.pagesIterator: %w", err)
	}

	for {
		page, _, isEnd, err := nextPage()
		if err != nil {
			return nil, fmt.Errorf("nextPage: %w", err)
		}

		if isEnd {
			return nil, ErrRecordNotFound()
		}

		for _, pointer := range page.Pointers {
			data := page.GetDataByPointer(pointer)
			record := DeserializeRecordBySchema(t.Schema, data)

			if match(record) {
				return record, nil
			}
		}
	}
}

func (t *Table) insertFirstPage(data []byte) error {
	page := page.NewEmptyPage()
	page.Insert(data)
	serialized := page.Serialize()

	if err := os.WriteFile(t.Path, serialized, os.ModeAppend); err != nil {
		return fmt.Errorf("os.WriteFile: %w", err)
	}

	return nil
}
func (t *Table) insertIntoNonemptyTable(data []byte) error {
	descriptor, nextPageFn, err := t.pagesIterator()
	if err != nil {
		return fmt.Errorf("Table.pagesIterator: %w", err)
	}
	defer descriptor.Close()

	for {
		page, pageOffset, isEnd, err := nextPageFn()
		if err != nil {
			return fmt.Errorf("nextPageIterFn: %w", err)
		}

		if isEnd {
			break
		}

		if !page.CheckSpace(len(data)) {
			continue
		}

		page.Insert(data)
		serialized := page.Serialize()

		if _, err := descriptor.WriteAt(serialized, pageOffset); err != nil {
			return fmt.Errorf("os.File.WriteAt: %w", err)
		}

		break
	}

	return nil
}

type nextPageIterFn func() (nextPage *page.Page, pageOffset int64, hasNext bool, err error)

func (t *Table) pagesIterator() (
	descriptor *os.File,
	nextPage nextPageIterFn,
	err error,
) {
	fileInfo, err := os.Stat(t.Path)
	if err != nil {
		return nil, nil, fmt.Errorf("os.Stat: %w", err)
	}

	numPages := fileInfo.Size() / page.PageSize

	file, err := os.OpenFile(t.Path, os.O_WRONLY, posixAccessRight)
	if err != nil {
		return nil, nil, fmt.Errorf("os.Open: %w", err)
	}

	var (
		pageStart int64
		pageNum   int64
	)

	return file, func() (
		nextPage *page.Page,
		pageOffset int64,
		hasNext bool,
		err error,
	) {
		if pageNum >= numPages {
			return nil, pageStart, false, nil
		}

		serialized := make([]byte, page.PageSize)
		if _, err := file.ReadAt(serialized, pageStart); err != nil {
			return nil, pageStart, false, fmt.Errorf("os.File.ReatAt: %w", err)
		}

		deserialized, err := page.DeserializePage(serialized)
		if err != nil {
			return nil, pageStart, false, fmt.Errorf("DeserializePage: %w", err)
		}

		pageNum++
		pageStart += page.PageSize

		return deserialized, pageStart, false, nil
	}, nil
}
