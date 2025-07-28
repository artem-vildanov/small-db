package page

import (
	"encoding/binary"
	"fmt"
	"os"
)

/*
Пейджа состоит из:

Заголовок (32 bytes)
Указатели на данные
Свободное место
Данные
*/

// размеры в байтах
const (
	PageSize = 8192 // 8 KB

	// NumSlots (2) + FreeSpaceStart (2) + FreeSpaceEnd (2) + Padding (26)
	PageHeaderSize = 32

	// NumSlots (2) + FreeSpaceStart (2) + FreeSpaceEnd (2)
	pageHeaderPayloadSize = 2 + 2 + 2
	PagePaddingSize       = PageHeaderSize - pageHeaderPayloadSize

	// Offset (2) + Size (2) + Status (1)
	ItemPointerSize = 5
)

// статусы строки
const (
	StatusDeleted byte = 0
	StatusActive  byte = 1
)

// 32 bytes
type PageHeader struct {
	NumSlots       uint16
	FreeSpaceStart uint16
	FreeSpaceEnd   uint16
}

// 5 bytes
type ItemPointer struct {
	Offset uint16
	Size   uint16
	Status uint8
}

func NewItemPointer(offset uint16, size uint16) *ItemPointer {
	return &ItemPointer{
		Offset: offset,
		Size:   size,
		Status: StatusActive,
	}
}

type Page struct {
	Header   *PageHeader
	Pointers []*ItemPointer
	RawPage  []byte
}

func NewEmptyPage() *Page {
	header := &PageHeader{
		NumSlots:       0,
		FreeSpaceStart: PageHeaderSize,
		FreeSpaceEnd:   PageSize,
	}

	page := &Page{
		Header: header,
	}

	page.RawPage = page.Serialize()

	return page
}

func DeserializePage(serialized []byte) (*Page, error) {
	if len(serialized) != PageSize {
		return nil, fmt.Errorf(
			"failed to deserialize page: serialized page size not equal to %d bytes",
			PageSize,
		)
	}

	deserialized := &Page{
		Header:  &PageHeader{},
		RawPage: serialized,
	}

	// header
	deserialized.Header.NumSlots = binary.BigEndian.Uint16(serialized[0:])
	deserialized.Header.FreeSpaceStart = binary.BigEndian.Uint16(serialized[2:])
	deserialized.Header.FreeSpaceEnd = binary.BigEndian.Uint16(serialized[4:])

	if deserialized.Header.NumSlots == 0 {
		return deserialized, nil
	}

	// pointers
	pointerOffset := PageHeaderSize
	for i := uint16(0); i < deserialized.Header.NumSlots; i++ {
		offset := binary.BigEndian.Uint16(serialized[pointerOffset:])
		pointerOffset += 2

		size := binary.BigEndian.Uint16(serialized[pointerOffset:])
		pointerOffset += 2

		status := uint8(serialized[pointerOffset])
		pointerOffset += 1

		deserialized.Pointers = append(deserialized.Pointers, &ItemPointer{
			Offset: offset,
			Size:   size,
			Status: status,
		})
	}

	return deserialized, nil
}

func (p *Page) Insert(data []byte) error {
	dataLen := uint16(len(data))

	totalFreeSpace := p.Header.FreeSpaceEnd - p.Header.FreeSpaceStart
	if totalFreeSpace < dataLen {
		return NewErrCantFitDataIntoPage(dataLen, totalFreeSpace)
	}

	p.Header.NumSlots += 1
	p.Header.FreeSpaceStart += ItemPointerSize
	p.Header.FreeSpaceEnd -= dataLen

	pointer := NewItemPointer(p.Header.FreeSpaceEnd, dataLen)
	p.Pointers = append(p.Pointers, pointer)

	copy(p.RawPage[p.Header.FreeSpaceEnd:], data)

	return nil
}

func (p *Page) Serialize() []byte {
	serialized := make([]byte, PageSize)

	// header
	binary.BigEndian.PutUint16(serialized[0:], p.Header.NumSlots)
	binary.BigEndian.PutUint16(serialized[2:], p.Header.FreeSpaceStart)
	binary.BigEndian.PutUint16(serialized[4:], p.Header.FreeSpaceEnd)

	if p.Header.NumSlots == 0 {
		return serialized
	}

	// pointers
	pointerOffset := PageHeaderSize
	for i := 0; i < len(p.Pointers); i++ {
		binary.BigEndian.PutUint16(serialized[pointerOffset:], p.Pointers[i].Offset)
		pointerOffset += 2

		binary.BigEndian.PutUint16(serialized[pointerOffset:], p.Pointers[i].Size)
		pointerOffset += 2

		serialized[pointerOffset] = p.Pointers[i].Status
		pointerOffset += 1
	}

	copy(serialized[pointerOffset:], p.RawPage[pointerOffset:])

	return serialized
}

func (p *Page) FreeSpaceMoreThanRequired(requiredSpace int) bool {
	freeSpace := p.Header.FreeSpaceEnd - p.Header.FreeSpaceStart
	return int(freeSpace) >= requiredSpace
}

func (p *Page) GetDataByPointer(pointer *ItemPointer) []byte {
	return p.RawPage[pointer.Offset : pointer.Offset+pointer.Size]
}

type pagesIterator struct {
	descriptor     *os.File
	pageOffset     int64
	numPages       int64
	currentPageNum int64
	reachedEnd     bool
}

func NewPagesIter(descriptor *os.File) (*pagesIterator, error) {
	fileInfo, err := descriptor.Stat()
	if err != nil {
		return nil, fmt.Errorf("os.Stat: %w", err)
	}

	numPages := fileInfo.Size() / PageSize

	return &pagesIterator{
		descriptor: descriptor,
		numPages:   numPages,
		pageOffset: -PageSize,
	}, nil
}

func (i *pagesIterator) Next() bool {
	i.pageOffset += PageSize

	if i.currentPageNum >= i.numPages {
		i.reachedEnd = true
		return false
	} else {
		i.currentPageNum++
		return true
	}
}

// обошли все страницы итератором
func (i *pagesIterator) ReachedEnd() bool {
	return i.reachedEnd 
}

func (i *pagesIterator) GetPage() (*Page, error) {
	serialized := make([]byte, PageSize)
	if _, err := i.descriptor.ReadAt(serialized, i.pageOffset); err != nil {
		return nil, fmt.Errorf("os.File.ReatAt: %w", err)
	}

	deserialized, err := DeserializePage(serialized)
	if err != nil {
		return nil, fmt.Errorf("DeserializePage: %w", err)
	}

	return deserialized, nil
}

func (i *pagesIterator) GetPageOffset() int64 {
	return i.pageOffset
}
