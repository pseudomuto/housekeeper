package consts

import "os"

const (
	// ModeDir is the standard file mode for creating directories
	ModeDir = os.FileMode(0o755)

	// ModeFile is the standard file mode for creating files
	ModeFile = os.FileMode(0o644)
)
