package cmd

import "go.uber.org/fx"

var Module = fx.Module("cli",
	fx.Provide(
		fx.Annotate(bootstrap, fx.ResultTags(`group:"commands"`)),
		fx.Annotate(dev, fx.ResultTags(`group:"commands"`)),
		fx.Annotate(diff, fx.ResultTags(`group:"commands"`)),
		fx.Annotate(fmtCmd, fx.ResultTags(`group:"commands"`)),
		fx.Annotate(initCmd, fx.ResultTags(`group:"commands"`)),
		fx.Annotate(rehash, fx.ResultTags(`group:"commands"`)),
		fx.Annotate(schema, fx.ResultTags(`group:"commands"`)),
		fx.Annotate(snapshot, fx.ResultTags(`group:"commands"`)),
	),
	fx.Invoke(Run),
)
