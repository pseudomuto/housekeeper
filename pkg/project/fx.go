package project

import "go.uber.org/fx"

var Module = fx.Module("project", fx.Provide(
	New,
))
