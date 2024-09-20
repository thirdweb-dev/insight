package log

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
)

func InitLogger() {
	// overrides zerolog global logger
	log.Logger = NewLogger("default")
}

func NewLogger(name string) zerolog.Logger {
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	level := zerolog.WarnLevel
	if lvl, err := zerolog.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil && lvl != zerolog.NoLevel {
		level = lvl
	}
	zerolog.SetGlobalLevel(level)

	prettify := os.Getenv("LOG_PRETTIFY") == "true"
	logger := zerolog.New(os.Stderr).With().Timestamp().Str("component", name).Logger()
	logger = logger.With().Caller().Logger()
	if prettify {
		logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	return logger
}
