package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_InsertAndGet(t *testing.T) {
	const filePath = "../data/file.data"
	var err error

	data1 := newTestData("asdfasdfasdf")
	data2 := newTestData("qwerqwer")
	data3 := newTestData("zxcvasdadf")
	data4 := newTestData("zxcvasdadfzxcvzx")

	table := NewTable(filePath)
	err = table.Insert(data1)
	require.NoError(t, err)

	err = table.Insert(data2)
	require.NoError(t, err)

	err = table.Insert(data3)
	require.NoError(t, err)

	err = table.Insert(data4)
	require.NoError(t, err)

	descriptor, nextPageFn, err := table.pagesIterator()
	require.NoError(t, err)
	defer descriptor.Close()

	var pageNum int
	for {
		page, pageOffset, isEnd, err := nextPageFn()
		require.NoError(t, err)

		if isEnd {
			assert.Equal(t, 2, pageNum)
			assert.Equal(t, PageSize * 3, pageOffset)
			break
		}

		if pageNum == 0 {
			require.Equal(t, len(page.Pointers), 3)

			// todo дописать тесты
			gotData1 := page.GetDataByPointer(page.Pointers[0])
		}
	}
}

func newTestData(data string) []byte {
	dataPadding := make([]byte, len(data)+PageSize/4)
	return append([]byte(data), dataPadding...)
}


