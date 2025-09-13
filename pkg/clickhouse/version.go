package clickhouse

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// VersionInfo represents parsed ClickHouse version information
type VersionInfo struct {
	Major int    // Major version number (e.g., 21)
	Minor int    // Minor version number (e.g., 10)
	Patch int    // Patch version number (e.g., 3)
	Raw   string // Raw version string from ClickHouse
}

// String returns the version as a string in format "major.minor.patch"
func (v VersionInfo) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// IsAtLeast checks if this version is at least the specified version
func (v VersionInfo) IsAtLeast(major, minor int) bool {
	if v.Major > major {
		return true
	}
	if v.Major == major && v.Minor >= minor {
		return true
	}
	return false
}

// SupportsOriginColumn returns true if this ClickHouse version supports the origin column in system.functions
// This feature was introduced in ClickHouse 21.10
func (v VersionInfo) SupportsOriginColumn() bool {
	return v.IsAtLeast(21, 10)
}

// GetVersion retrieves and parses the ClickHouse version from the server
func (c *Client) GetVersion(ctx context.Context) (*VersionInfo, error) {
	query := "SELECT version()"
	row := c.conn.QueryRow(ctx, query)

	var versionStr string
	if err := row.Scan(&versionStr); err != nil {
		return nil, errors.Wrap(err, "failed to query ClickHouse version")
	}

	version, err := parseVersion(versionStr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse ClickHouse version: %s", versionStr)
	}

	return version, nil
}

// parseVersion parses a ClickHouse version string into structured information
// ClickHouse version strings can be in various formats:
// - "21.10.3.9" (standard)
// - "21.10.3.9-testing" (with suffix)
// - "21.10.3.9 (official build)" (with description)
func parseVersion(versionStr string) (*VersionInfo, error) {
	// Clean the version string by removing common suffixes and descriptions
	cleaned := strings.TrimSpace(versionStr)

	// Remove everything after the first space (e.g., "(official build)")
	if spaceIdx := strings.Index(cleaned, " "); spaceIdx != -1 {
		cleaned = cleaned[:spaceIdx]
	}

	// Remove common suffixes like "-testing", "-stable", etc.
	if dashIdx := strings.Index(cleaned, "-"); dashIdx != -1 {
		cleaned = cleaned[:dashIdx]
	}

	// Use regex to extract version components
	// Matches patterns like: 21.10.3.9, 21.10.3, 21.10
	versionRegex := regexp.MustCompile(`^(\d+)\.(\d+)(?:\.(\d+))?(?:\.(\d+))?`)
	matches := versionRegex.FindStringSubmatch(cleaned)

	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid version format: %s", versionStr)
	}

	major, err := strconv.Atoi(matches[1])
	if err != nil {
		return nil, fmt.Errorf("invalid major version: %s", matches[1])
	}

	minor, err := strconv.Atoi(matches[2])
	if err != nil {
		return nil, fmt.Errorf("invalid minor version: %s", matches[2])
	}

	patch := 0
	if len(matches) > 3 && matches[3] != "" {
		patch, err = strconv.Atoi(matches[3])
		if err != nil {
			return nil, fmt.Errorf("invalid patch version: %s", matches[3])
		}
	}

	return &VersionInfo{
		Major: major,
		Minor: minor,
		Patch: patch,
		Raw:   versionStr,
	}, nil
}
