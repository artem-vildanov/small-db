package internal

import (
	"fmt"
	"os"
)

const (
	posixAccessRight = 0644
)

type Table struct {
	filePath string
}

func NewTable(filePath string) *Table {
	return &Table{filePath}
}

type nextPageIterFn func() (nextPage *Page, pageOffset int64, hasNext bool, err error)

func (t *Table) pagesIterator() (
	descriptor *os.File,
	nextPage nextPageIterFn,
	err error,
) {
	fileInfo, err := os.Stat(t.filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("os.Stat: %w", err)
	}

	numPages := fileInfo.Size() / PageSize

	file, err := os.OpenFile(t.filePath, os.O_WRONLY, posixAccessRight)
	if err != nil {
		return nil, nil, fmt.Errorf("os.Open: %w", err)
	}

	var (
		pageStart int64
		pageNum   int64
	)

	return file, func() (*Page, int64, bool, error) {
		if pageNum >= numPages {
			return nil, pageStart, false, nil
		}

		serialized := make([]byte, PageSize)
		if _, err := file.ReadAt(serialized, pageStart); err != nil {
			return nil, pageStart, false, fmt.Errorf("os.File.ReatAt: %w", err)
		}

		deserialized, err := DeserializePage(serialized)
		if err != nil {
			return nil, pageStart, false, fmt.Errorf("DeserializePage: %w", err)
		}

		pageNum++
		pageStart += PageSize

		return deserialized, pageStart, false, nil
	}, nil
}

func (t *Table) GetAll() ([][]byte, error) {
	descriptor, nextPageFn, err := t.pagesIterator()
	if err != nil {
		return nil, fmt.Errorf("Table.pagesIterator: %w", err)
	}
	defer descriptor.Close()

	var result [][]byte
	for {
		page, _, isEnd, err := nextPageFn()
		if err != nil {
			return nil, fmt.Errorf("nextPageIterFn: %w", err)
		}

		if isEnd {
			break
		}

		for _, pointer := range page.Pointers {
			result = append(result, page.GetDataByPointer(pointer))
		}
	}

	return result, nil
}

func (t *Table) Insert(data []byte) error {
	fileInfo, err := os.Stat(t.filePath)
	if err != nil {
		return fmt.Errorf("os.Stat: %w", err)
	}

	numPages := fileInfo.Size() / PageSize
	if numPages == 0 {
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

func (t *Table) insertFirstPage(data []byte) error {
	page := NewEmptyPage()
	page.Insert(data)
	serialized := page.Serialize()

	if err := os.WriteFile(t.filePath, serialized, os.ModeAppend); err != nil {
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

// func (t *Table) insertIntoNonemptyTable(data []byte, numPages int64) error {
// 	file, err := os.OpenFile(t.filePath, os.O_WRONLY, posixAccessRight)
// 	if err != nil {
// 		return fmt.Errorf("os.Open: %w", err)
// 	}
// 	defer file.Close()
//
// 	var (
// 		pageStart int64 = 0
// 		pageEnd   int64 = PageSize
// 	)
//
// 	for i := int64(0); i < numPages; i++ {
// 		serialized := make([]byte, PageSize)
// 		if _, err := file.ReadAt(serialized, pageStart); err != nil {
// 			return fmt.Errorf("os.File.ReatAt: %w", err)
// 		}
//
// 		deserialized, err := DeserializePage(serialized[pageStart:pageEnd])
// 		if err != nil {
// 			return fmt.Errorf("DeserializePage: %w", err)
// 		}
//
// 		if !deserialized.CheckSpace(len(data)) {
// 			continue
// 		}
//
// 		deserialized.Insert(data)
// 		serialized = deserialized.Serialize()
//
// 		if _, err := file.WriteAt(serialized, pageStart); err != nil {
// 			return fmt.Errorf("os.File.WriteAt: %w", err)
// 		}
// 	}
//
// 	return fmt.Errorf("not enough space to insert data with size %d", len(data))
// }
