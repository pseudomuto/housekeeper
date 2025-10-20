package compare

// NilCheck performs a nil check on two pointers and returns whether they are equal
// and whether more comparison checks are needed.
//
// Returns (equal, needsMoreChecks) where:
//   - equal: true if both are nil, false if only one is nil
//   - needsMoreChecks: true if both pointers are non-nil and further comparison is needed
//
// Example:
//
//	func (e *Expression) Equal(other *Expression) bool {
//	    if eq, needsMoreChecks := compare.NilCheck(e, other); !needsMoreChecks {
//	        return eq
//	    }
//	    // Continue with field comparisons...
//	}
func NilCheck[T any](a, b *T) (equal bool, needsMoreChecks bool) {
	if a == nil && b == nil {
		return true, false
	}
	if a == nil || b == nil {
		return false, false
	}
	return false, true
}

// Pointers compares two pointer values for equality.
// Returns true if both are nil, or both are non-nil with equal values.
//
// Example:
//
//	func (t *TypeParameter) Equal(other *TypeParameter) bool {
//	    return compare.Pointers(t.Number, other.Number) &&
//	           compare.Pointers(t.String, other.String)
//	}
func Pointers[T comparable](a, b *T) bool {
	if (a != nil) != (b != nil) {
		return false
	}
	if a != nil && *a != *b {
		return false
	}
	return true
}

// PointersWithEqual compares two pointers using a custom equality function.
// Returns true if both are nil, or both are non-nil and the equality function returns true.
//
// Example:
//
//	func (t *TableEngine) Equal(other *TableEngine) bool {
//	    return compare.PointersWithEqual(t.Engine, other.Engine,
//	        func(a, b *EngineSpec) bool { return a.Equal(b) })
//	}
func PointersWithEqual[T any](a, b *T, equalFunc func(*T, *T) bool) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return equalFunc(a, b)
}

// Slices compares two slices for equality using an equality function for elements.
// Returns true if both slices have the same length and all corresponding elements are equal.
//
// Example:
//
//	func (t *Tuple) Equal(other *Tuple) bool {
//	    return compare.Slices(t.Elements, other.Elements,
//	        func(a, b TupleElement) bool { return a.Equal(&b) })
//	}
func Slices[T any](a, b []T, equalFunc func(T, T) bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equalFunc(a[i], b[i]) {
			return false
		}
	}
	return true
}

// SlicesUnordered compares two slices for equality regardless of order.
// Returns true if both slices contain the same elements (by the equality function).
//
// Example:
//
//	func (s *Settings) Equal(other *Settings) bool {
//	    return compare.SlicesUnordered(s.Items, other.Items,
//	        func(a, b Setting) bool { return a.Name == b.Name && a.Value == b.Value })
//	}
func SlicesUnordered[T any](a, b []T, equalFunc func(T, T) bool) bool {
	if len(a) != len(b) {
		return false
	}

	// Track which elements in b have been matched
	matched := make([]bool, len(b))

	for _, aElem := range a {
		found := false
		for j, bElem := range b {
			if !matched[j] && equalFunc(aElem, bElem) {
				matched[j] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// Maps compares two maps for equality.
// Returns true if both maps have the same keys and all corresponding values are equal.
//
// Example:
//
//	func (t *TableInfo) Equal(other *TableInfo) bool {
//	    return compare.Maps(t.Settings, other.Settings)
//	}
func Maps[K comparable, V comparable](a, b map[K]V) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// MapsWithEqual compares two maps using a custom equality function for values.
// Returns true if both maps have the same keys and all corresponding values are equal
// according to the equality function.
//
// Example:
//
//	func (d *Dictionary) Equal(other *Dictionary) bool {
//	    return compare.MapsWithEqual(d.Columns, other.Columns,
//	        func(a, b *Column) bool { return a.Equal(b) })
//	}
func MapsWithEqual[K comparable, V any](a, b map[K]V, equalFunc func(V, V) bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		bv, ok := b[k]
		if !ok || !equalFunc(v, bv) {
			return false
		}
	}
	return true
}
