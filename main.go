package main

import (
	"os"

	"github.com/lyeith/eventbus/internal/app"
	"github.com/rs/zerolog/log"
)

func main() {
	if err := app.Run(); err != nil {
		log.Error().Err(err).Msg("EventBus failed")
		os.Exit(1)
	}
}
