package errors

type Error int

func (e Error) Error() string {
	return errorMsg[int(e)]
}

const (
	// -------- Store --------

	// -------- Volume --------
	msgVolumeNotExist = 2001

	// -------- Data --------

	// -------- Index --------

	// -------- Needle --------
	msgNeedleNotExist = 5001

	// -------- StoreServer --------

)

var (
	errorMsg = map[int]string{
		// -------- Store --------

		// -------- Volume --------
		msgVolumeNotExist: "Volume not exist.",

		// -------- Data --------

		// -------- Index --------

		// -------- Needle --------
		msgNeedleNotExist: "Needle not exist.",

		// -------- StoreServer --------
	}
)

var (
	// -------- Store --------

	// -------- Volume --------
	ErrVolumeNotExist = Error(msgVolumeNotExist)

	// -------- Data --------

	// -------- Index --------

	// -------- Needle --------
	ErrNeedleNotExist = Error(msgNeedleNotExist)

	// -------- StoreServer --------
)
