package config

import (
	"os"

	"github.com/pseudomuto/housekeeper/pkg/format"
	"go.uber.org/fx"
)

var Module = fx.Module("config", fx.Provide(
	// Function attempts to load the configuration from housekeeper.yaml if it exists.
	// Returns nil if the file doesn't exist, allowing commands that don't require config
	// (like init, help, version) to function properly.
	func() (*Config, error) {
		// Check if housekeeper.yaml exists
		if _, err := os.Stat("housekeeper.yaml"); os.IsNotExist(err) {
			// Return nil config for commands that don't need it
			return nil, nil
		}

		// Load and return the config
		return LoadConfigFile("housekeeper.yaml")
	},
	func(c *Config) *format.Formatter {
		return c.GetFormatter()
	},
))
