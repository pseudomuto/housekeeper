package schema

import (
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestRefreshClausesAreEqual(t *testing.T) {
	tests := []struct {
		name     string
		refresh1 *parser.RefreshClause
		refresh2 *parser.RefreshClause
		expected bool
	}{
		{
			name:     "both nil",
			refresh1: nil,
			refresh2: nil,
			expected: true,
		},
		{
			name:     "first nil second not nil",
			refresh1: nil,
			refresh2: &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
			expected: false,
		},
		{
			name:     "first not nil second nil",
			refresh1: &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
			refresh2: nil,
			expected: false,
		},
		{
			name:     "equal EVERY clauses",
			refresh1: &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
			refresh2: &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
			expected: true,
		},
		{
			name:     "equal AFTER clauses",
			refresh1: &parser.RefreshClause{After: true, Interval: 1, Unit: "HOUR"},
			refresh2: &parser.RefreshClause{After: true, Interval: 1, Unit: "HOUR"},
			expected: true,
		},
		{
			name:     "different EVERY vs AFTER",
			refresh1: &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
			refresh2: &parser.RefreshClause{After: true, Interval: 10, Unit: "SECONDS"},
			expected: false,
		},
		{
			name:     "different intervals",
			refresh1: &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
			refresh2: &parser.RefreshClause{Every: true, Interval: 30, Unit: "SECONDS"},
			expected: false,
		},
		{
			name:     "different units",
			refresh1: &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
			refresh2: &parser.RefreshClause{Every: true, Interval: 10, Unit: "MINUTES"},
			expected: false,
		},
		{
			name:     "case insensitive units",
			refresh1: &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
			refresh2: &parser.RefreshClause{Every: true, Interval: 10, Unit: "seconds"},
			expected: true,
		},
		{
			name: "equal with OFFSET",
			refresh1: &parser.RefreshClause{
				Every:          true,
				Interval:       1,
				Unit:           "DAY",
				OffsetInterval: intPtr(6),
				OffsetUnit:     stringPtr("HOURS"),
			},
			refresh2: &parser.RefreshClause{
				Every:          true,
				Interval:       1,
				Unit:           "DAY",
				OffsetInterval: intPtr(6),
				OffsetUnit:     stringPtr("HOURS"),
			},
			expected: true,
		},
		{
			name: "different OFFSET intervals",
			refresh1: &parser.RefreshClause{
				Every:          true,
				Interval:       1,
				Unit:           "DAY",
				OffsetInterval: intPtr(6),
				OffsetUnit:     stringPtr("HOURS"),
			},
			refresh2: &parser.RefreshClause{
				Every:          true,
				Interval:       1,
				Unit:           "DAY",
				OffsetInterval: intPtr(12),
				OffsetUnit:     stringPtr("HOURS"),
			},
			expected: false,
		},
		{
			name: "one has OFFSET other does not",
			refresh1: &parser.RefreshClause{
				Every:          true,
				Interval:       1,
				Unit:           "DAY",
				OffsetInterval: intPtr(6),
				OffsetUnit:     stringPtr("HOURS"),
			},
			refresh2: &parser.RefreshClause{
				Every:    true,
				Interval: 1,
				Unit:     "DAY",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := refreshClausesAreEqual(tt.refresh1, tt.refresh2)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestViewStatementsAreEqual_RefreshAndAppend(t *testing.T) {
	tests := []struct {
		name     string
		stmt1    *parser.CreateViewStmt
		stmt2    *parser.CreateViewStmt
		expected bool
	}{
		{
			name: "both have same refresh and append",
			stmt1: &parser.CreateViewStmt{
				Materialized: true,
				Name:         "mv_test",
				Refresh:      &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
				Append:       true,
				To:           &parser.ViewTableTarget{Table: stringPtr("target")},
				AsSelect:     &parser.SelectStatement{},
			},
			stmt2: &parser.CreateViewStmt{
				Materialized: true,
				Name:         "mv_test",
				Refresh:      &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
				Append:       true,
				To:           &parser.ViewTableTarget{Table: stringPtr("target")},
				AsSelect:     &parser.SelectStatement{},
			},
			expected: true,
		},
		{
			name: "different refresh intervals",
			stmt1: &parser.CreateViewStmt{
				Materialized: true,
				Name:         "mv_test",
				Refresh:      &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
				Append:       true,
				To:           &parser.ViewTableTarget{Table: stringPtr("target")},
				AsSelect:     &parser.SelectStatement{},
			},
			stmt2: &parser.CreateViewStmt{
				Materialized: true,
				Name:         "mv_test",
				Refresh:      &parser.RefreshClause{Every: true, Interval: 30, Unit: "SECONDS"},
				Append:       true,
				To:           &parser.ViewTableTarget{Table: stringPtr("target")},
				AsSelect:     &parser.SelectStatement{},
			},
			expected: false,
		},
		{
			name: "one has refresh other does not",
			stmt1: &parser.CreateViewStmt{
				Materialized: true,
				Name:         "mv_test",
				Refresh:      &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
				To:           &parser.ViewTableTarget{Table: stringPtr("target")},
				AsSelect:     &parser.SelectStatement{},
			},
			stmt2: &parser.CreateViewStmt{
				Materialized: true,
				Name:         "mv_test",
				To:           &parser.ViewTableTarget{Table: stringPtr("target")},
				AsSelect:     &parser.SelectStatement{},
			},
			expected: false,
		},
		{
			name: "different append flags",
			stmt1: &parser.CreateViewStmt{
				Materialized: true,
				Name:         "mv_test",
				Refresh:      &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
				Append:       true,
				To:           &parser.ViewTableTarget{Table: stringPtr("target")},
				AsSelect:     &parser.SelectStatement{},
			},
			stmt2: &parser.CreateViewStmt{
				Materialized: true,
				Name:         "mv_test",
				Refresh:      &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
				Append:       false,
				To:           &parser.ViewTableTarget{Table: stringPtr("target")},
				AsSelect:     &parser.SelectStatement{},
			},
			expected: false,
		},
		{
			name: "neither has refresh or append",
			stmt1: &parser.CreateViewStmt{
				Materialized: true,
				Name:         "mv_test",
				To:           &parser.ViewTableTarget{Table: stringPtr("target")},
				AsSelect:     &parser.SelectStatement{},
			},
			stmt2: &parser.CreateViewStmt{
				Materialized: true,
				Name:         "mv_test",
				To:           &parser.ViewTableTarget{Table: stringPtr("target")},
				AsSelect:     &parser.SelectStatement{},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := viewStatementsAreEqual(tt.stmt1, tt.stmt2)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatRefreshClause(t *testing.T) {
	tests := []struct {
		name     string
		refresh  *parser.RefreshClause
		expected string
	}{
		{
			name:     "nil refresh",
			refresh:  nil,
			expected: "",
		},
		{
			name:     "EVERY seconds",
			refresh:  &parser.RefreshClause{Every: true, Interval: 10, Unit: "SECONDS"},
			expected: "REFRESH EVERY 10 SECONDS",
		},
		{
			name:     "AFTER hour",
			refresh:  &parser.RefreshClause{After: true, Interval: 1, Unit: "HOUR"},
			expected: "REFRESH AFTER 1 HOUR",
		},
		{
			name: "EVERY day with OFFSET",
			refresh: &parser.RefreshClause{
				Every:          true,
				Interval:       1,
				Unit:           "DAY",
				OffsetInterval: intPtr(6),
				OffsetUnit:     stringPtr("HOURS"),
			},
			expected: "REFRESH EVERY 1 DAY OFFSET 6 HOURS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatRefreshClause(tt.refresh)
			require.Equal(t, tt.expected, result)
		})
	}
}

func intPtr(i int) *int {
	return &i
}
