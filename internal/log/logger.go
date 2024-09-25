package log

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	config "github.com/thirdweb-dev/indexer/configs"
)

func InitLogger() {
	// overrides zerolog global logger
	log.Logger = NewLogger("default")
}

func NewLogger(name string) zerolog.Logger {
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	level := zerolog.WarnLevel
	if lvl, err := zerolog.ParseLevel(config.Cfg.Log.Level); err == nil && lvl != zerolog.NoLevel {
		level = lvl
	}
	zerolog.SetGlobalLevel(level)

	logger := zerolog.New(os.Stderr).With().Timestamp().Str("component", name).Logger()
	logger = logger.With().Caller().Logger()
	if config.Cfg.Log.Prettify {
		logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	return logger
}
