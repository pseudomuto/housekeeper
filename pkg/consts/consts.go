package consts

import "os"

const (
	// ModeDir is the standard file mode for creating directories
	ModeDir = os.FileMode(0o755)

	// ModeFile is the standard file mode for creating files
	ModeFile = os.FileMode(0o644)

	// DefaultClickHouseVersion is the default ClickHouse version used when none is specified
	DefaultClickHouseVersion = "25.7"

	// DefaultClickHouseConfigDir is the default directory for ClickHouse configuration files
	DefaultClickHouseConfigDir = "db/config.d"

	// DefaultClickHouseCluster is the default cluster name used when none is specified
	DefaultClickHouseCluster = "cluster"
)
