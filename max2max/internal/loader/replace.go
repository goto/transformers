package loader

import (
	"log/slog"
)

type replaceLoader struct {
	logger *slog.Logger
}

func NewReplaceLoader(logger *slog.Logger) *replaceLoader {
	return &replaceLoader{
		logger: logger,
	}
}

func (l *replaceLoader) GetQuery(tableID, query string) string {
	return "-- TODO replace loader"
}

func (l *replaceLoader) GetPartitionedQuery(tableID, query string, partitionName []string) string {
	return "-- TODO replace loader"
}
