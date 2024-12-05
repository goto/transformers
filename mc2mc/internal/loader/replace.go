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
	headers, qr := SeparateHeadersAndQuery(query)
	return fmt.Sprintf("%s INSERT OVERWRITE TABLE %s %s;", headers, tableID, qr)
}

func (l *replaceLoader) GetPartitionedQuery(tableID, query string, partitionNames []string) string {
	headers, qr := SeparateHeadersAndQuery(query)
	return fmt.Sprintf("%s INSERT OVERWRITE TABLE %s PARTITION (%s) %s;", headers, tableID, strings.Join(partitionNames, ", "), qr)
}
