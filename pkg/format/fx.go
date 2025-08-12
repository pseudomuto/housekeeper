package format

import "go.uber.org/fx"

var Module = fx.Module("format", fx.Provide(
	New,
))
