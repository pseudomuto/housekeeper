package project

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// ParseSchema compiles and parses the schema defined in the project configuration.
// It processes the entrypoint file defined in the project configuration,
// recursively resolving any import directives (-- housekeeper:import)
// and returns a parsed grammar containing all DDL statements.
//
// Example:
//
//	project := project.New("/path/to/project")
//	if err := project.Initialize(); err != nil {
//		log.Fatal(err)
//	}
//
//	// Parse project schema
//	grammar, err := project.ParseSchema()
//	if err != nil {
//		log.Fatal("Failed to parse schema:", err)
//	}
//
//	// Process the parsed statements
//	for _, stmt := range grammar.Statements {
//		if stmt.CreateTable != nil {
//			fmt.Printf("Found table: %s\n", stmt.CreateTable.Name)
//		}
//	}
func (p *Project) ParseSchema() (*parser.SQL, error) {
	var g *parser.SQL
	err := p.withConfig(func(cfg *Config) error {
		var buf bytes.Buffer
		if err := compileSchema(cfg.Entrypoint, &buf); err != nil {
			return errors.Wrapf(err, "failed to load schema from: %s", cfg.Entrypoint)
		}

		var err error
		g, err = parser.ParseString(buf.String())
		return errors.Wrap(err, "failed to parse schema SQL")
	})

	return g, err
}

// compileSchema recursively compiles an Atlas schema file and its imports. It processes import directives (lines
// starting with "-- housekeeper:import") and includes the referenced files' contents in the output. The function changes the
// working directory to properly resolve relative import paths.
func compileSchema(path string, w io.Writer) error {
	pwd, _ := os.Getwd()
	defer func() { _ = os.Chdir(pwd) }()

	dir := filepath.Dir(path)
	if err := os.Chdir(dir); err != nil {
		return errors.Wrapf(err, "failed to cd to %s", dir)
	}

	f, err := os.Open(filepath.Base(path))
	if err != nil {
		return errors.Wrapf(err, "failed to read file %s", path)
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "-- housekeeper:import") {
			parts := strings.Split(line, " ")
			if err := compileSchema(parts[len(parts)-1], w); err != nil {
				return err
			}

			continue
		}

		fmt.Fprintln(w, line)
	}

	return errors.Wrapf(scanner.Err(), "failed scanning %s", path)
}
