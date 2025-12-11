package parser_test

import (
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestCodecSpec(t *testing.T) {
	t.Parallel()

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			spec     CodecSpec
			expected string
		}{
			{
				name:     "no parameters",
				spec:     CodecSpec{Name: "ZSTD"},
				expected: "ZSTD",
			},
			{
				name: "single parameter",
				spec: CodecSpec{
					Name:       "ZSTD",
					Parameters: []TypeParameter{{Number: utils.Ptr("3")}},
				},
				expected: "ZSTD(3)",
			},
			{
				name: "multiple parameters",
				spec: CodecSpec{
					Name:       "T64",
					Parameters: []TypeParameter{{Number: utils.Ptr("1")}, {Number: utils.Ptr("2")}},
				},
				expected: "T64(1, 2)",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tt.expected, tt.spec.String())
			})
		}
	})

	t.Run("Equal", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			a, b     CodecSpec
			expected bool
		}{
			{
				name:     "same name no params",
				a:        CodecSpec{Name: "ZSTD"},
				b:        CodecSpec{Name: "ZSTD"},
				expected: true,
			},
			{
				name:     "different names",
				a:        CodecSpec{Name: "ZSTD"},
				b:        CodecSpec{Name: "LZ4"},
				expected: false,
			},
			{
				name: "same params",
				a: CodecSpec{
					Name:       "ZSTD",
					Parameters: []TypeParameter{{Number: utils.Ptr("3")}},
				},
				b: CodecSpec{
					Name:       "ZSTD",
					Parameters: []TypeParameter{{Number: utils.Ptr("3")}},
				},
				expected: true,
			},
			{
				name: "different params",
				a: CodecSpec{
					Name:       "ZSTD",
					Parameters: []TypeParameter{{Number: utils.Ptr("1")}},
				},
				b: CodecSpec{
					Name:       "ZSTD",
					Parameters: []TypeParameter{{Number: utils.Ptr("3")}},
				},
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tt.expected, tt.a.Equal(&tt.b))
			})
		}
	})
}

func TestCodecClause(t *testing.T) {
	t.Parallel()

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			clause   *CodecClause
			expected string
		}{
			{
				name:     "nil clause",
				clause:   nil,
				expected: "",
			},
			{
				name:     "empty codecs",
				clause:   &CodecClause{},
				expected: "",
			},
			{
				name: "single codec",
				clause: &CodecClause{
					Codecs: []CodecSpec{{Name: "ZSTD"}},
				},
				expected: "CODEC(ZSTD)",
			},
			{
				name: "multiple codecs",
				clause: &CodecClause{
					Codecs: []CodecSpec{{Name: "Delta"}, {Name: "ZSTD"}},
				},
				expected: "CODEC(Delta, ZSTD)",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tt.expected, tt.clause.String())
			})
		}
	})

	t.Run("Equal", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			a, b     *CodecClause
			expected bool
		}{
			{
				name:     "both nil",
				a:        nil,
				b:        nil,
				expected: true,
			},
			{
				name:     "nil vs non-nil",
				a:        nil,
				b:        &CodecClause{Codecs: []CodecSpec{{Name: "ZSTD"}}},
				expected: false,
			},
			{
				name:     "non-nil vs nil",
				a:        &CodecClause{Codecs: []CodecSpec{{Name: "ZSTD"}}},
				b:        nil,
				expected: false,
			},
			{
				name:     "same single codec",
				a:        &CodecClause{Codecs: []CodecSpec{{Name: "ZSTD"}}},
				b:        &CodecClause{Codecs: []CodecSpec{{Name: "ZSTD"}}},
				expected: true,
			},
			{
				name:     "different codecs",
				a:        &CodecClause{Codecs: []CodecSpec{{Name: "ZSTD"}}},
				b:        &CodecClause{Codecs: []CodecSpec{{Name: "LZ4"}}},
				expected: false,
			},
			{
				name:     "different count",
				a:        &CodecClause{Codecs: []CodecSpec{{Name: "Delta"}, {Name: "ZSTD"}}},
				b:        &CodecClause{Codecs: []CodecSpec{{Name: "ZSTD"}}},
				expected: false,
			},
			{
				name:     "different order",
				a:        &CodecClause{Codecs: []CodecSpec{{Name: "Delta"}, {Name: "ZSTD"}}},
				b:        &CodecClause{Codecs: []CodecSpec{{Name: "ZSTD"}, {Name: "Delta"}}},
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tt.expected, tt.a.Equal(tt.b))
			})
		}
	})
}
