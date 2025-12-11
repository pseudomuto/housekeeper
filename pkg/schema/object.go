package schema

// SchemaObject is the interface implemented by all schema object Info types
// (DatabaseInfo, TableInfo, DictionaryInfo, ViewInfo, FunctionInfo, RoleInfo).
//
// This interface enables generic algorithms for rename detection and comparison
// across all object types without type-specific code duplication.
type SchemaObject interface {
	// GetName returns the fully-qualified name of the schema object.
	// For objects with database prefixes (e.g., tables), this returns "db.name".
	// For global objects (e.g., databases), this returns just the name.
	GetName() string

	// GetCluster returns the cluster name if the object is clustered,
	// or an empty string if not clustered.
	GetCluster() string

	// PropertiesMatch returns true if this object has the same properties
	// as the other object, excluding the name. This is used for rename detection:
	// if two objects have different names but matching properties, it's a rename.
	//
	// The other parameter is guaranteed to be the same concrete type as the receiver.
	PropertiesMatch(other SchemaObject) bool
}
