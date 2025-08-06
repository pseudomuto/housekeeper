package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/urfave/cli/v3"
)

// NB: These are set by GoReleaser during a build.
var (
	version string
	commit  string
	date    string
)

func main() {
	cli.VersionPrinter = func(cmd *cli.Command) {
		fmt.Fprintln(cmd.Writer, "Version:", version)
		fmt.Fprintln(cmd.Writer, "Commit:", commit)
		fmt.Fprintln(cmd.Writer, "Date:", date)
	}

	app := &cli.Command{
		Name:  "housekeeper",
		Usage: "A tool for managing ClickHouse schema migrations",
		Description: `housekeeper is a CLI tool that helps you manage ClickHouse database 
schema migrations by comparing desired schema definitions with the current 
database state and generating appropriate migration files.`,
		Version: version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "the housekeeper config file",
				Sources: cli.EnvVars("HOUSEKEEPER_CONFIG"),
				Value:   "housekeeper.yaml",
			},
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
