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

func (p *Project) LoadMigrationSet(env string) (*MigrationSet, error) {
	var ms *MigrationSet
	err := p.withEnv(env, func(e *Env) error {
		ms = &MigrationSet{}

		// Get absolute path to migrations directory
		migrationsDir := filepath.Join(p.root, e.Dir)

		entries, err := os.ReadDir(e.Dir)
		if err != nil {
			return errors.Wrapf(err, "failed to read dir: %s", e.Dir)
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			if strings.EqualFold(entry.Name(), sumFileName) {
				sumPath := filepath.Join(e.Dir, entry.Name())
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

			ms.files = append(ms.files, filepath.Join(migrationsDir, entry.Name()))
		}

		// Sort files in lexicographical order for consistent sum file generation
		slices.Sort(ms.files)

		return nil
	})

	return ms, err
}
