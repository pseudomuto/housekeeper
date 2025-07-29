package main

import (
	"context"
	"log"
	"os"

	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:  "housekeeper",
		Usage: "A tool for managing ClickHouse schema migrations",
		Description: `housekeeper is a CLI tool that helps you manage ClickHouse database 
schema migrations by comparing desired schema definitions with the current 
database state and generating appropriate migration files.`,
		Version: "1.0.0",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "config file (default is $HOME/.housekeeper.json)",
			},
			&cli.StringFlag{
				Name:    "dsn",
				Aliases: []string{"d"},
				Usage:   "ClickHouse DSN",
				Value:   "localhost:9000",
			},
		},
		Commands: []*cli.Command{
			diffCommand(),
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
