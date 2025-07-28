package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPage_SerializeDeserialize(t *testing.T) {
	// помещаем немного данных
	t.Run("success", func(t *testing.T) {
		var (
			err error
			data1 = []byte("hello world")
			data2 = []byte("xzcvzxcv")
			data3 = []byte("asdfasdfasdfasdf")
		)

		page := NewEmptyPage()

		err = page.Insert(data1)
		require.NoError(t, err)

		err = page.Insert(data2)
		require.NoError(t, err)

		err = page.Insert(data3)
		require.NoError(t, err)

		serialized := page.Serialize()

		deserialized, err := DeserializePage(serialized)
		require.NoError(t, err)

		assert.Equal(t, page.Header, deserialized.Header)
		assert.Equal(t, page.Pointers, deserialized.Pointers)

		var (
			gotData1 = deserialized.GetDataByPointer(deserialized.Pointers[0])
			gotData2 = deserialized.GetDataByPointer(deserialized.Pointers[1])
			gotData3 = deserialized.GetDataByPointer(deserialized.Pointers[2])
		)

		assert.Equal(t, data1, gotData1)
		assert.Equal(t, data2, gotData2)
		assert.Equal(t, data3, gotData3)
	})

	t.Run("помещаем слишком много данных", func(t *testing.T) {
		var (
			err error
			data1 = make([]byte, 4000)
			data2 = make([]byte, 5000)
		)

		page := NewEmptyPage()

		err = page.Insert(data1)
		require.NoError(t, err)

		err = page.Insert(data2)
		assert.EqualError(t, err, "cant fit data into page")

		serialized := page.Serialize()

		deserialized, err := DeserializePage(serialized)
		require.NoError(t, err)

		assert.Equal(t, 1, len(deserialized.Pointers))

		var (
			gotData1 = deserialized.GetDataByPointer(deserialized.Pointers[0])
		)

		assert.Equal(t, data1, gotData1)
	})
}