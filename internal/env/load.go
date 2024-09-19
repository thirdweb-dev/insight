package env

import (
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
)

func Load() {
	err := godotenv.Load()
	if err != nil {
		log.Error().Err(err).Msg("error loading .env file")
	}
}
