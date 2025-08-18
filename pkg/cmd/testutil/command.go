package testutil

import (
	"context"
	"testing"

	"github.com/urfave/cli/v3"
)

// Note: fx-based testing helpers are commented out to avoid import cycles
// when testing within the cmd package. These can be uncommented and used
// if testing from an external package.

// TestVersion represents version information for testing
// type TestVersion struct {
// 	Version   string
// 	Commit    string
// 	Timestamp string
// }

// CommandTestApp creates a test fx application with command dependencies
// type CommandTestApp struct {
// 	App       *fx.App
// 	Fixture   *ProjectFixture
// 	Lifecycle *fxtest.Lifecycle
// 	t         *testing.T
// }

// TestApp creates a test fx application with all command dependencies
// func TestApp(t *testing.T, fixture *ProjectFixture) *CommandTestApp {
// 	t.Helper()
// 	return nil
// }

// RunCommand executes a command with test context
func RunCommand(t *testing.T, command *cli.Command, args []string) error {
	t.Helper()

	ctx := context.Background()

	// Create a test CLI app
	app := &cli.Command{
		Name:     "test",
		Commands: []*cli.Command{command},
	}

	// Prepend command name to args
	fullArgs := append([]string{"test", command.Name}, args...)

	return app.Run(ctx, fullArgs)
}

// RunCommandWithContext executes a command with a custom context
func RunCommandWithContext(ctx context.Context, t *testing.T, command *cli.Command, args []string) error {
	t.Helper()

	// Create a test CLI app
	app := &cli.Command{
		Name:     "test",
		Commands: []*cli.Command{command},
	}

	// Prepend command name to args
	fullArgs := append([]string{"test", command.Name}, args...)

	return app.Run(ctx, fullArgs)
}

// ParseCommandFlags parses command line flags for a command
func ParseCommandFlags(t *testing.T, command *cli.Command, args []string) (*cli.Command, error) {
	t.Helper()

	ctx := context.Background()

	// Create a copy of the command with the flags set
	cmdCopy := &cli.Command{
		Name:   command.Name,
		Flags:  command.Flags,
		Action: command.Action,
	}

	// Create a test CLI app
	app := &cli.Command{
		Name:     "test",
		Commands: []*cli.Command{cmdCopy},
	}

	// Prepend command name to args
	fullArgs := append([]string{"test", command.Name}, args...)

	// Run the app but with a no-op action to just parse flags
	origAction := cmdCopy.Action
	cmdCopy.Action = func(ctx context.Context, cmd *cli.Command) error {
		// No-op, just for parsing
		return nil
	}

	err := app.Run(ctx, fullArgs)
	if err != nil {
		return nil, err
	}

	// Restore original action
	cmdCopy.Action = origAction

	return cmdCopy, nil
}

// TestCommandExecution runs a command and verifies it executes without error
func TestCommandExecution(t *testing.T, command *cli.Command, args []string) error {
	t.Helper()
	return RunCommand(t, command, args)
}

// TestCommandExecutionWithError runs a command and returns the error
func TestCommandExecutionWithError(t *testing.T, command *cli.Command, args []string) error {
	t.Helper()
	return RunCommand(t, command, args)
}

// CreateTestCommand creates a simple test command for testing purposes
func CreateTestCommand(name string, action func(context.Context, *cli.Command) error) *cli.Command {
	return &cli.Command{
		Name:   name,
		Action: action,
	}
}

// CreateTestCommandWithFlags creates a test command with flags
func CreateTestCommandWithFlags(name string, flags []cli.Flag, action func(context.Context, *cli.Command) error) *cli.Command {
	return &cli.Command{
		Name:   name,
		Flags:  flags,
		Action: action,
	}
}
