package loader

import (
	"fmt"
	"log/slog"
	"strings"
)

type appendLoader struct {
	logger *slog.Logger
}

func NewAppendLoader(logger *slog.Logger) (*appendLoader, error) {
	return &appendLoader{
		logger: logger,
	}, nil
}

func (l *appendLoader) GetQuery(tableID, query string) string {
	headers, qr := SeparateHeadersAndQuery(query)
	return fmt.Sprintf("%s INSERT INTO TABLE %s %s;", headers, tableID, qr)
}

func (l *appendLoader) GetPartitionedQuery(tableID, query string, partitionNames []string) string {
	headers, qr := SeparateHeadersAndQuery(query)
	return fmt.Sprintf("%s INSERT INTO TABLE %s PARTITION (%s) %s;", headers, tableID, strings.Join(partitionNames, ", "), qr)
}
