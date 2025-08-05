# Contributing to Housekeeper

Thank you for your interest in contributing to Housekeeper! This guide will help you understand the project structure and how to contribute effectively.

## Project Structure

```
housekeeper/
├── cmd/
│   └── housekeeper/     # CLI application
├── pkg/
│   ├── clickhouse/      # ClickHouse client
│   ├── migrator/        # Migration generation
│   └── parser/          # DDL parser
├── examples/            # Example schema files
└── docs/               # Documentation
```

## Development Setup

1. Clone the repository
2. Install Go 1.21 or later
3. Run tests: `go test ./...`
4. Build: `go build ./cmd/housekeeper`

## Code Style

Follow the conventions in [CLAUDE.md](CLAUDE.md):
- Use `goimports` for formatting
- Order code elements: package, imports, const, var, type, functions
- Use `github.com/pkg/errors` for error handling
- Write table-driven tests with embedded testdata

## Parser Development

The parser package provides a robust participle-based parser for ClickHouse DDL statements.

### Architecture

- **`parser.go`** - Main parser logic, lexer, and grammar types
- **`database.go`** - All database DDL statement parsing (CREATE, ALTER, ATTACH, DETACH, DROP)
- **`parser_test.go`** - Comprehensive testdata-driven testing system

### Adding New DDL Support

1. Define grammar types in the appropriate file (database.go, or create table.go, view.go, etc.)
2. Add parsing rules to the Grammar type in parser.go
3. Implement processing functions
4. Add comprehensive tests using the testdata system
5. Update documentation

### Testing the Parser

The parser uses embedded testdata files for testing:

```bash
# Run all parser tests
go test -v ./pkg/parser

# Run specific test file
go test -v -run TestParserWithTestdata/database_create.sql ./pkg/parser

# Update YAML expectations from SQL
go test -v -run TestParserWithTestdata -update ./pkg/parser
```

#### Test Structure

Each test consists of:
- `.sql` file with DDL statements to parse
- `.yaml` file with expected parsing results

Example `testdata/example.yaml`:
```yaml
databases:
  db_name:
    name: string          # Database name
    cluster: string       # ON CLUSTER value (empty if not specified)
    engine: string        # ENGINE value (empty if not specified)
    comment: string       # COMMENT value (empty if not specified)
```

#### Adding Parser Tests

1. Create a new SQL file in `pkg/parser/testdata/`:
```sql
CREATE DATABASE test_db ENGINE = Atomic COMMENT 'Test database';
```

2. Generate the YAML file using -update:
```bash
go test -v -run TestParserWithTestdata/your_test.sql -update
```

3. Verify the generated YAML matches expectations

## Migrator Development

The migrator package generates database migrations by comparing schemas.

### Adding New Migration Features

1. Implement comparison logic in `database.go`
2. Add migration generation in `generator.go`
3. Return `ErrUnsupported` for operations that can't be automated
4. Add tests in `testdata/` with YAML expectations

### Testing Migrations

```bash
# Run all migrator tests
go test -v ./pkg/migrator

# Update test expectations
go test -v -run TestMigrationGeneration -update-migration
```

## Testing Guidelines

1. **Write table-driven tests** - Use subtests for different scenarios
2. **Use testdata** - Embed test files for complex inputs
3. **Test error cases** - Ensure proper error handling
4. **Mock external dependencies** - Don't require a real ClickHouse instance for unit tests

Example test structure:
```go
func TestFeature(t *testing.T) {
    tests := []struct {
        name    string
        input   string
        want    string
        wantErr bool
    }{
        {
            name:  "valid input",
            input: "CREATE DATABASE test",
            want:  "expected output",
        },
        {
            name:    "invalid input",
            input:   "INVALID SQL",
            wantErr: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got, err := Feature(tt.input)
            if tt.wantErr {
                require.Error(t, err)
                return
            }
            require.NoError(t, err)
            require.Equal(t, tt.want, got)
        })
    }
}
```

## Submitting Changes

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make your changes following the code style
4. Add tests for new functionality
5. Ensure all tests pass: `go test ./...`
6. Update documentation as needed
7. Commit with descriptive messages
8. Push to your fork and create a pull request

## Pull Request Guidelines

- Provide a clear description of the changes
- Reference any related issues
- Ensure CI passes
- Keep changes focused - one feature per PR
- Update tests and documentation

## Reporting Issues

When reporting issues, please include:
- ClickHouse version
- Go version
- Minimal reproduction steps
- Expected vs actual behavior
- Any relevant SQL or error messages

## Future Development Areas

Areas where contributions are particularly welcome:

1. **Table Operations** - Add CREATE/ALTER/DROP TABLE support
2. **View Operations** - Add CREATE/DROP VIEW support
3. **Migration Execution** - Add migration runner functionality
4. **Schema Validation** - Add semantic validation of DDL
5. **Performance** - Optimize parser for large schemas

## Questions?

Feel free to open an issue for discussion or clarification on any aspect of the project.