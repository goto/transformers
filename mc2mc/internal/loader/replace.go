package loader

import (
	"fmt"
	"log/slog"
	"strings"
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
	return fmt.Sprintf("INSERT OVERWRITE TABLE %s %s;", tableID, query)
}

func (l *replaceLoader) GetPartitionedQuery(tableID, query string, partitionNames []string) string {
	return fmt.Sprintf("INSERT OVERWRITE TABLE %s PARTITION (%s) %s;", tableID, strings.Join(partitionNames, ", "), query)
}
