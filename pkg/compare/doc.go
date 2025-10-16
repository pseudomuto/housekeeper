// Package compare provides generic comparison utilities for structural equality testing.
//
// This package offers a set of helper functions that eliminate boilerplate code when
// implementing Equal() methods on structs. It handles common patterns like nil checking,
// pointer comparisons, slice comparisons, and map comparisons.
//
// # Key Features
//
//   - Generic functions that work with any type
//   - Nil-safe pointer comparisons
//   - Slice comparisons with custom equality functions
//   - Map comparisons with value equality
//   - Reduces boilerplate code by 60-80%
//
// # Usage Examples
//
// Replace repetitive nil checks:
//
//	// Before (6 lines):
//	if x == nil && other == nil {
//	    return true
//	}
//	if x == nil || other == nil {
//	    return false
//	}
//
//	// After (2 lines):
//	if eq, done := compare.NilCheck(x, other); !done {
//	    return eq
//	}
//
// Compare pointer fields:
//
//	// Before (12 lines for 2 fields):
//	if (t.Field1 != nil) != (other.Field1 != nil) {
//	    return false
//	}
//	if t.Field1 != nil && *t.Field1 != *other.Field1 {
//	    return false
//	}
//	// ... repeat for Field2
//
//	// After (2 lines):
//	return compare.Pointers(t.Field1, other.Field1) &&
//	       compare.Pointers(t.Field2, other.Field2)
//
// Compare slices with element equality:
//
//	// Before (8 lines):
//	if len(a.Items) != len(other.Items) {
//	    return false
//	}
//	for i := range a.Items {
//	    if !a.Items[i].Equal(&other.Items[i]) {
//	        return false
//	    }
//	}
//	return true
//
//	// After (3 lines):
//	return compare.Slices(a.Items, other.Items, func(x, y Item) bool {
//	    return x.Equal(&y)
//	})
package compare
