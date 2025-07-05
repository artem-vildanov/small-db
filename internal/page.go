package internal

import (
	"encoding/binary"
	"fmt"
)

/*
Пейджа состоит из:

Header 32 bytes
[]ItemPointer N * 4 bytes
FreeSpace
[]DataTuple N * M bytes
*/

// размеры в байтах
const (
	PageSize       = 8192 // 8 KB
	PageHeaderSize = 32

	// NumSlots + FreeSpaceStart + FreeSpaceEnd
	pageHeaderPayloadSize = 2 + 2 + 2
	PagePaddingSize       = PageHeaderSize - pageHeaderPayloadSize

	ItemPointerSize = 4
)

// 32 bytes
type PageHeader struct {
	NumSlots       uint16
	FreeSpaceStart uint16
	FreeSpaceEnd   uint16
}

// 4 bytes
type ItemPointer struct {
	Offset uint16
	Size   uint16
}

func NewItemPointer(offset uint16, size uint16) *ItemPointer {
	return &ItemPointer{
		Offset: offset,
		Size:   size,
	}
}

type Page struct {
	Header   *PageHeader
	Pointers []*ItemPointer
	RawPage []byte
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

		deserialized.Pointers = append(deserialized.Pointers, &ItemPointer{
			Offset: offset,
			Size:   size,
		})
	}

	return deserialized, nil
}

func (p *Page) Insert(data []byte) {
	dataLen := uint16(len(data))

	p.Header.NumSlots += 1
	p.Header.FreeSpaceStart += ItemPointerSize
	p.Header.FreeSpaceEnd -= dataLen

	pointer := NewItemPointer(p.Header.FreeSpaceEnd, dataLen)
	p.Pointers = append(p.Pointers, pointer)

	copy(p.RawPage[pointer.Offset:], data)
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
	}

	return serialized
}

func (p *Page) CheckSpace(requiredSpace int) bool {
	freeSpace := p.Header.FreeSpaceEnd - p.Header.FreeSpaceStart
	return int(freeSpace) >= requiredSpace
}

func (p *Page) GetDataByPointer(pointer *ItemPointer) []byte {
	return p.RawPage[pointer.Offset : pointer.Offset+pointer.Size]
}
