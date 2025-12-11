package utils

// Ptr returns a pointer to the provided value v.
// This is useful for creating pointers to literals or temporary values.
func Ptr[T any](v T) *T {
	return &v
}
