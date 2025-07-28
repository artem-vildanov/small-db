package consts

import "os"

const (
	// права доступа к файлу:
	// владелец может читать и писать
	// остальные только читать
	PosixAccessRight = 0644

	// сочетание флагов:
	// создать файл, если не существует (O_CREATE)
	// ошибка, если файл уже существует (O_EXCL)
	// открыть для чтения и записи (O_RDWR)
	CreateIfNotExists = os.O_CREATE | os.O_EXCL | os.O_RDWR

	DataExtension = ".data"
	JsonExtension = ".json"
)

