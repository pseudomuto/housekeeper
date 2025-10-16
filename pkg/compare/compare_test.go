package compare_test

import (
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/compare"
	"github.com/stretchr/testify/require"
)

func TestNilCheck(t *testing.T) {
	tests := []struct {
		name             string
		a, b             *int
		expectedEqual    bool
		expectedContinue bool
	}{
		{
			name:             "both nil",
			a:                nil,
			b:                nil,
			expectedEqual:    true,
			expectedContinue: false,
		},
		{
			name:             "first nil",
			a:                nil,
			b:                intPtr(5),
			expectedEqual:    false,
			expectedContinue: false,
		},
		{
			name:             "second nil",
			a:                intPtr(5),
			b:                nil,
			expectedEqual:    false,
			expectedContinue: false,
		},
		{
			name:             "neither nil",
			a:                intPtr(5),
			b:                intPtr(5),
			expectedEqual:    false,
			expectedContinue: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			equal, shouldContinue := NilCheck(tt.a, tt.b)
			require.Equal(t, tt.expectedEqual, equal)
			require.Equal(t, tt.expectedContinue, shouldContinue)
		})
	}
}

func TestPointers(t *testing.T) {
	tests := []struct {
		name     string
		a, b     *int
		expected bool
	}{
		{
			name:     "both nil",
			a:        nil,
			b:        nil,
			expected: true,
		},
		{
			name:     "first nil",
			a:        nil,
			b:        intPtr(5),
			expected: false,
		},
		{
			name:     "second nil",
			a:        intPtr(5),
			b:        nil,
			expected: false,
		},
		{
			name:     "equal values",
			a:        intPtr(5),
			b:        intPtr(5),
			expected: true,
		},
		{
			name:     "different values",
			a:        intPtr(5),
			b:        intPtr(10),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Pointers(tt.a, tt.b)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestPointersWithEqual(t *testing.T) {
	type testStruct struct {
		value int
	}

	equalFunc := func(a, b *testStruct) bool {
		return a.value == b.value
	}

	tests := []struct {
		name     string
		a, b     *testStruct
		expected bool
	}{
		{
			name:     "both nil",
			a:        nil,
			b:        nil,
			expected: true,
		},
		{
			name:     "first nil",
			a:        nil,
			b:        &testStruct{value: 5},
			expected: false,
		},
		{
			name:     "second nil",
			a:        &testStruct{value: 5},
			b:        nil,
			expected: false,
		},
		{
			name:     "equal by function",
			a:        &testStruct{value: 5},
			b:        &testStruct{value: 5},
			expected: true,
		},
		{
			name:     "not equal by function",
			a:        &testStruct{value: 5},
			b:        &testStruct{value: 10},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PointersWithEqual(tt.a, tt.b, equalFunc)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSlices(t *testing.T) {
	equalFunc := func(a, b int) bool { return a == b }

	tests := []struct {
		name     string
		a, b     []int
		expected bool
	}{
		{
			name:     "both empty",
			a:        []int{},
			b:        []int{},
			expected: true,
		},
		{
			name:     "both nil",
			a:        nil,
			b:        nil,
			expected: true,
		},
		{
			name:     "different lengths",
			a:        []int{1, 2, 3},
			b:        []int{1, 2},
			expected: false,
		},
		{
			name:     "equal elements",
			a:        []int{1, 2, 3},
			b:        []int{1, 2, 3},
			expected: true,
		},
		{
			name:     "different elements",
			a:        []int{1, 2, 3},
			b:        []int{1, 2, 4},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Slices(tt.a, tt.b, equalFunc)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSlicesUnordered(t *testing.T) {
	equalFunc := func(a, b int) bool { return a == b }

	tests := []struct {
		name     string
		a, b     []int
		expected bool
	}{
		{
			name:     "both empty",
			a:        []int{},
			b:        []int{},
			expected: true,
		},
		{
			name:     "different lengths",
			a:        []int{1, 2, 3},
			b:        []int{1, 2},
			expected: false,
		},
		{
			name:     "same elements same order",
			a:        []int{1, 2, 3},
			b:        []int{1, 2, 3},
			expected: true,
		},
		{
			name:     "same elements different order",
			a:        []int{3, 1, 2},
			b:        []int{1, 2, 3},
			expected: true,
		},
		{
			name:     "different elements",
			a:        []int{1, 2, 3},
			b:        []int{1, 2, 4},
			expected: false,
		},
		{
			name:     "duplicates handled correctly",
			a:        []int{1, 2, 2, 3},
			b:        []int{2, 1, 3, 2},
			expected: true,
		},
		{
			name:     "duplicates mismatch",
			a:        []int{1, 2, 2, 3},
			b:        []int{1, 2, 3, 3},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SlicesUnordered(tt.a, tt.b, equalFunc)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestMaps(t *testing.T) {
	tests := []struct {
		name     string
		a, b     map[string]int
		expected bool
	}{
		{
			name:     "both empty",
			a:        map[string]int{},
			b:        map[string]int{},
			expected: true,
		},
		{
			name:     "both nil",
			a:        nil,
			b:        nil,
			expected: true,
		},
		{
			name:     "different sizes",
			a:        map[string]int{"a": 1, "b": 2},
			b:        map[string]int{"a": 1},
			expected: false,
		},
		{
			name:     "same keys and values",
			a:        map[string]int{"a": 1, "b": 2},
			b:        map[string]int{"a": 1, "b": 2},
			expected: true,
		},
		{
			name:     "different values",
			a:        map[string]int{"a": 1, "b": 2},
			b:        map[string]int{"a": 1, "b": 3},
			expected: false,
		},
		{
			name:     "different keys",
			a:        map[string]int{"a": 1, "b": 2},
			b:        map[string]int{"a": 1, "c": 2},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Maps(tt.a, tt.b)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestMapsWithEqual(t *testing.T) {
	type testStruct struct {
		value int
	}

	equalFunc := func(a, b testStruct) bool {
		return a.value == b.value
	}

	tests := []struct {
		name     string
		a, b     map[string]testStruct
		expected bool
	}{
		{
			name:     "both empty",
			a:        map[string]testStruct{},
			b:        map[string]testStruct{},
			expected: true,
		},
		{
			name:     "same keys and equal values",
			a:        map[string]testStruct{"a": {value: 1}, "b": {value: 2}},
			b:        map[string]testStruct{"a": {value: 1}, "b": {value: 2}},
			expected: true,
		},
		{
			name:     "different values",
			a:        map[string]testStruct{"a": {value: 1}, "b": {value: 2}},
			b:        map[string]testStruct{"a": {value: 1}, "b": {value: 3}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MapsWithEqual(tt.a, tt.b, equalFunc)
			require.Equal(t, tt.expected, result)
		})
	}
}

func intPtr(i int) *int {
	return &i
}
