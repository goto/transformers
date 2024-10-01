package loader

import (
	"fmt"
	"log/slog"
)

type replaceMergeLoader struct {
	logger *slog.Logger
}

func NewReplaceMergeLoader(logger *slog.Logger) (*replaceMergeLoader, error) {
	return &replaceMergeLoader{
		logger: logger,
	}, nil
}

func (l *replaceMergeLoader) GetQuery(tableID, query string) string {
	return fmt.Sprintf("INSERT OVERWRITE TABLE %s %s", tableID, query)
}

func (l *replaceMergeLoader) GetPartitionedQuery(tableID, query string, partitionName []string) string {
	return "-- TODO merge replace loader"
}
