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
	msgDataNomoreSpace = 3001

	// -------- Index --------
	msgIndexNomoreSpace = 4001

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
		msgDataNomoreSpace: "No more space in data file",

		// -------- Index --------
		msgIndexNomoreSpace: "No more space in index file.",

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
	ErrDataNomoreSpace = Error(msgDataNomoreSpace)

	// -------- Index --------
	ErrIndexNomoreSpace = Error(msgIndexNomoreSpace)

	// -------- Needle --------
	ErrNeedleNotExist = Error(msgNeedleNotExist)

	// -------- StoreServer --------
)
