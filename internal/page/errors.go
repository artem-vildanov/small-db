package page

type ErrCantFitDataIntoPage struct {
	DataLen          uint16
	PageFreeSpaceLen uint16
	Message          string
}

func NewErrCantFitDataIntoPage(dataLen, pageFreeSpaceLen uint16) error {
	return &ErrCantFitDataIntoPage{
		DataLen:          dataLen,
		PageFreeSpaceLen: pageFreeSpaceLen,
		Message:          "cant fit data into page",
	}
}

func (e *ErrCantFitDataIntoPage) Error() string {
	return e.Message
}
