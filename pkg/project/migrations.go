package project

import (
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
)

const sumFileName = "housekeeper.sum"

type (
	MigrationSet struct {
		files []string
		sum   *migrator.SumFile
	}
)

// Files returns the list of migration files in lexicographical order.
func (ms *MigrationSet) Files() []string {
	return ms.files
}

// Sum returns the loaded SumFile if present, otherwise nil.
func (ms *MigrationSet) Sum() *migrator.SumFile {
	return ms.sum
}

// GenerateSumFile creates a new SumFile based on the current migration files in the set.
// Files are processed in lexicographical order to ensure consistent hash generation.
// Returns an error if any file cannot be read.
func (ms *MigrationSet) GenerateSumFile() (*migrator.SumFile, error) {
	sumFile := migrator.NewSumFile()

	for _, filePath := range ms.files {
		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read migration file: %s", filePath)
		}

		// Use just the filename (not the full path) for the sum file
		fileName := filepath.Base(filePath)
		sumFile.AddFile(fileName, content)
	}

	return sumFile, nil
}

// IsValid validates the MigrationSet by comparing the generated SumFile's TotalHash
// with the loaded SumFile's TotalHash. Returns true if:
// 1. Both the loaded sum and generated sum exist
// 2. Their TotalHash values are equal
// Returns false if there's no loaded sum file, or if the hashes don't match.
func (ms *MigrationSet) IsValid() (bool, error) {
	if ms.sum == nil {
		return false, nil
	}

	generatedSum, err := ms.GenerateSumFile()
	if err != nil {
		return false, errors.Wrap(err, "failed to generate sum file for validation")
	}

	// Ensure both sum files have their TotalHash computed by writing to buffer
	var buf1, buf2 strings.Builder
	_, err = ms.sum.WriteTo(&buf1)
	if err != nil {
		return false, errors.Wrap(err, "failed to compute loaded sum file hash")
	}

	_, err = generatedSum.WriteTo(&buf2)
	if err != nil {
		return false, errors.Wrap(err, "failed to compute generated sum file hash")
	}

	return ms.sum.TotalHash == generatedSum.TotalHash, nil
}

// LoadMigrationSet loads all migration files from the project migrations directory.
// It creates a MigrationSet containing all .sql files found in the migrations directory
// defined in the configuration, along with any SumFile for integrity checking.
//
// Migration files are sorted lexicographically to ensure consistent ordering.
// This method also loads the SumFile (if present) which contains SHA256 hashes
// of all migration files for integrity validation. The SumFile can be used to
// detect if any migration files have been modified since the last hash generation.
//
// Example:
//
//	project := project.New("/path/to/project")
//	if err := project.Initialize(); err != nil {
//		log.Fatal(err)
//	}
//
//	// Load migration set
//	migrationSet, err := project.LoadMigrationSet()
//	if err != nil {
//		log.Fatal("Failed to load migration set:", err)
//	}
//
//	// Check migration file integrity
//	isValid, err := migrationSet.IsValid()
//	if err != nil {
//		log.Fatal("Failed to validate migration set:", err)
//	}
//
//	if !isValid {
//		log.Println("Warning: Migration files have been modified")
//	}
//
//	// Process migration files
//	for _, file := range migrationSet.Files() {
//		fmt.Printf("Migration: %s\n", file)
//	}
func (p *Project) LoadMigrationSet() (*MigrationSet, error) {
	var ms *MigrationSet
	err := p.withConfig(func(cfg *Config) error {
		ms = &MigrationSet{}

		// Get absolute path to migrations directory
		migrationsDir := filepath.Join(p.root, cfg.Dir)

		entries, err := os.ReadDir(cfg.Dir)
		if err != nil {
			return errors.Wrapf(err, "failed to read dir: %s", cfg.Dir)
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			if strings.EqualFold(entry.Name(), sumFileName) {
				sumPath := filepath.Join(cfg.Dir, entry.Name())
				file, err := os.Open(sumPath)
				if err != nil {
					return errors.Wrapf(err, "failed to open sum file: %s", sumPath)
				}
				defer func() { _ = file.Close() }()

				sumFile, err := migrator.LoadSumFile(file)
				if err != nil {
					return errors.Wrapf(err, "failed to load sum file: %s", sumPath)
				}

				ms.sum = sumFile
				continue
			}

			// Only add .sql files to the migration set
			if strings.HasSuffix(entry.Name(), ".sql") {
				ms.files = append(ms.files, filepath.Join(migrationsDir, entry.Name()))
			}
		}

		// Sort files in lexicographical order for consistent sum file generation
		slices.Sort(ms.files)

		return nil
	})

	return ms, err
}
