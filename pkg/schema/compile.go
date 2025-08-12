package schema

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

// Compile recursively compiles a schema file and its imports. It processes import directives (lines
// starting with "-- housekeeper:import") and includes the referenced files' contents in the output.
// Import paths are resolved relative to the current file's directory.
//
// Example:
//
//	var buf bytes.Buffer
//	err := schema.Compile("db/main.sql", &buf)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// The compiled schema is now in buf
//	compiledSQL := buf.String()
//
//	// Parse the compiled schema
//	sql, err := parser.ParseString(compiledSQL)
//	if err != nil {
//		log.Fatal(err)
//	}
func Compile(path string, w io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		return errors.Wrapf(err, "failed to read file %s", path)
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "-- housekeeper:import") {
			parts := strings.Split(line, " ")
			importPath := parts[len(parts)-1]

			// Resolve import path relative to current file's directory
			if !filepath.IsAbs(importPath) {
				dir := filepath.Dir(path)
				importPath = filepath.Join(dir, importPath)
			}

			if err := Compile(importPath, w); err != nil {
				return err
			}

			continue
		}

		fmt.Fprintln(w, line)
	}

	return errors.Wrapf(scanner.Err(), "failed scanning %s", path)
}
