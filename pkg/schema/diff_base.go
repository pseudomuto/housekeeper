package schema

// DiffBase contains the common fields shared by all diff types
// (DatabaseDiff, TableDiff, DictionaryDiff, ViewDiff, FunctionDiff, RoleDiff).
//
// Embedding this struct in diff types eliminates the need to implement
// GetDiffType() and GetUpSQL() methods on each type individually.
//
// Example usage:
//
//	type DatabaseDiff struct {
//	    DiffBase
//	    // Database-specific fields...
//	}
type DiffBase struct {
	// Type is the operation type (CREATE, ALTER, DROP, RENAME, REPLACE, etc.)
	Type string

	// Name is the name of the object being modified
	Name string

	// NewName is the new name for rename operations (empty otherwise)
	NewName string

	// Description is a human-readable description of the change
	Description string

	// UpSQL is the SQL to apply the change (forward migration)
	UpSQL string

	// DownSQL is the SQL to rollback the change (reverse migration)
	DownSQL string
}

// GetDiffType implements diffProcessor interface
func (d *DiffBase) GetDiffType() string {
	return d.Type
}

// GetUpSQL implements diffProcessor interface
func (d *DiffBase) GetUpSQL() string {
	return d.UpSQL
}
