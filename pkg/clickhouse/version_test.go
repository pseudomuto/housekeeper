package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseVersion(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *VersionInfo
		wantErr  bool
	}{
		{
			name:  "standard version format",
			input: "21.10.3.9",
			expected: &VersionInfo{
				Major: 21,
				Minor: 10,
				Patch: 3,
				Raw:   "21.10.3.9",
			},
		},
		{
			name:  "version with official build",
			input: "21.10.3.9 (official build)",
			expected: &VersionInfo{
				Major: 21,
				Minor: 10,
				Patch: 3,
				Raw:   "21.10.3.9 (official build)",
			},
		},
		{
			name:  "version with testing suffix",
			input: "22.8.2.11-testing",
			expected: &VersionInfo{
				Major: 22,
				Minor: 8,
				Patch: 2,
				Raw:   "22.8.2.11-testing",
			},
		},
		{
			name:  "minimal version format",
			input: "20.3",
			expected: &VersionInfo{
				Major: 20,
				Minor: 3,
				Patch: 0,
				Raw:   "20.3",
			},
		},
		{
			name:  "three component version",
			input: "25.7.1",
			expected: &VersionInfo{
				Major: 25,
				Minor: 7,
				Patch: 1,
				Raw:   "25.7.1",
			},
		},
		{
			name:    "invalid version format",
			input:   "invalid",
			wantErr: true,
		},
		{
			name:    "empty version",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseVersion(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, result)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tt.expected.Major, result.Major)
			assert.Equal(t, tt.expected.Minor, result.Minor)
			assert.Equal(t, tt.expected.Patch, result.Patch)
			assert.Equal(t, tt.expected.Raw, result.Raw)
		})
	}
}

func TestVersionInfo_String(t *testing.T) {
	v := &VersionInfo{
		Major: 21,
		Minor: 10,
		Patch: 3,
		Raw:   "21.10.3.9",
	}

	assert.Equal(t, "21.10.3", v.String())
}

func TestVersionInfo_IsAtLeast(t *testing.T) {
	tests := []struct {
		name     string
		version  *VersionInfo
		major    int
		minor    int
		expected bool
	}{
		{
			name:     "version is exactly the target",
			version:  &VersionInfo{Major: 21, Minor: 10, Patch: 0},
			major:    21,
			minor:    10,
			expected: true,
		},
		{
			name:     "version is higher major",
			version:  &VersionInfo{Major: 22, Minor: 5, Patch: 0},
			major:    21,
			minor:    10,
			expected: true,
		},
		{
			name:     "version is same major, higher minor",
			version:  &VersionInfo{Major: 21, Minor: 12, Patch: 0},
			major:    21,
			minor:    10,
			expected: true,
		},
		{
			name:     "version is lower major",
			version:  &VersionInfo{Major: 20, Minor: 15, Patch: 0},
			major:    21,
			minor:    10,
			expected: false,
		},
		{
			name:     "version is same major, lower minor",
			version:  &VersionInfo{Major: 21, Minor: 8, Patch: 0},
			major:    21,
			minor:    10,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.version.IsAtLeast(tt.major, tt.minor)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestVersionInfo_SupportsOriginColumn(t *testing.T) {
	tests := []struct {
		name     string
		version  *VersionInfo
		expected bool
	}{
		{
			name:     "ClickHouse 21.10 supports origin column",
			version:  &VersionInfo{Major: 21, Minor: 10, Patch: 0},
			expected: true,
		},
		{
			name:     "ClickHouse 22.x supports origin column",
			version:  &VersionInfo{Major: 22, Minor: 1, Patch: 0},
			expected: true,
		},
		{
			name:     "ClickHouse 21.9 does not support origin column",
			version:  &VersionInfo{Major: 21, Minor: 9, Patch: 5},
			expected: false,
		},
		{
			name:     "ClickHouse 20.x does not support origin column",
			version:  &VersionInfo{Major: 20, Minor: 12, Patch: 0},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.version.SupportsOriginColumn()
			assert.Equal(t, tt.expected, result)
		})
	}
}
