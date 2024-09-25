package loader

import (
	"log/slog"
)

type replaceAllLoader struct {
	logger *slog.Logger
}

func NewReplaceAllLoader(logger *slog.Logger) *replaceAllLoader {
	return &replaceAllLoader{
		logger: logger,
	}
}

func (l *replaceAllLoader) GetQuery(tableID, query string) string {
	return "-- TODO replace all loader"
}
