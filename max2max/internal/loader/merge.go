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

func (l *mergeLoader) GetQuery(_, query string) string {
	return query
}

func (l *mergeLoader) GetPartitionedQuery(_, query string, _ []string) string {
	return query
}
