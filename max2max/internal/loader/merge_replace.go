package loader

import (
	"log/slog"
)

type mergeReplaceLoader struct {
	logger *slog.Logger
}

func NewMergeReplaceLoader(logger *slog.Logger) *mergeReplaceLoader {
	return &mergeReplaceLoader{
		logger: logger,
	}
}

func (l *mergeReplaceLoader) GetQuery(tableID, query string) string {
	return "-- TODO merge replace loader"
}
