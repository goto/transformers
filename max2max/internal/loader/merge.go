package loader

import (
	"log/slog"
)

type mergeLoader struct {
	logger *slog.Logger
}

func NewMergeLoader(logger *slog.Logger) *mergeLoader {
	return &mergeLoader{
		logger: logger,
	}
}

func (l *mergeLoader) GetQuery(tableID, query string) string {
	return "-- TODO merge loader"
}

func (l *mergeLoader) GetPartitionedQuery(tableID, partitionName, query string) string {
	return "-- TODO merge loader"
}
