package schema

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	A "github.com/IBM/fp-go/v2/array"
	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	file "github.com/IBM/fp-go/v2/ioeither/file"
	R "github.com/IBM/fp-go/v2/result"
)

var readDir = IOE.Eitherize1(os.ReadDir)

// Compile recursively compiles a schema entrypoint and writes the result to w.
//
// The entrypoint may be:
//   - A single .sql file, optionally containing -- housekeeper:import directives
//   - A directory, in which case all *.sql files are compiled in alphabetical order
//
// Import paths inside a file are resolved relative to that file's directory:
//
//	-- housekeeper:import tables/users.sql
func Compile(path string, w io.Writer) error {
	return R.ToError(F.Pipe1(
		file.Stat(path)(),
		R.Chain(func(info os.FileInfo) R.Result[struct{}] {
			if info.IsDir() {
				return R.TryCatchError(struct{}{}, compileDir(path, w))
			}
			return R.TryCatchError(struct{}{}, compileFile(path, w))
		}),
	))
}

// compileDir compiles all *.sql files in dir in alphabetical order.
// Sub-directories are ignored; use -- housekeeper:import for explicit cross-directory includes.
func compileDir(dir string, w io.Writer) error {
	return R.ToError(F.Pipe1(
		readDir(dir)(),
		R.Chain(func(entries []os.DirEntry) R.Result[struct{}] {
			sqlFiles := A.Filter(func(e os.DirEntry) bool {
				return !e.IsDir() && strings.HasSuffix(e.Name(), ".sql")
			})(entries)
			return F.Pipe1(
				R.TraverseArray(func(e os.DirEntry) R.Result[struct{}] {
					return R.TryCatchError(struct{}{}, compileFile(filepath.Join(dir, e.Name()), w))
				})(sqlFiles),
				R.Map(F.Constant1[[]struct{}, struct{}](struct{}{})),
			)
		}),
	))
}

// compileFile reads path, processes -- housekeeper:import directives recursively,
// and writes all non-directive lines to w.
func compileFile(path string, w io.Writer) error {
	return R.ToError(F.Pipe2(
		file.ReadAll(file.Open(path))(),
		R.Map(func(b []byte) []string {
			return strings.Split(strings.TrimRight(string(b), "\r\n"), "\n")
		}),
		R.Chain(func(lines []string) R.Result[struct{}] {
			return F.Pipe1(
				R.TraverseArray(processLine(path, w))(lines),
				R.Map(F.Constant1[[]struct{}, struct{}](struct{}{})),
			)
		}),
	))
}

// processLine returns a function that either emits line to w (for regular SQL lines) or
// recursively compiles the referenced file (for -- housekeeper:import directives).
// Import paths are resolved relative to the directory containing path.
func processLine(path string, w io.Writer) func(string) R.Result[struct{}] {
	return func(line string) R.Result[struct{}] {
		if !strings.HasPrefix(line, "-- housekeeper:import") {
			fmt.Fprintln(w, line)
			return R.Of(struct{}{})
		}
		parts := strings.Split(line, " ")
		importPath := parts[len(parts)-1]
		if !filepath.IsAbs(importPath) {
			importPath = filepath.Join(filepath.Dir(path), importPath)
		}
		return R.TryCatchError(struct{}{}, Compile(importPath, w))
	}
}
