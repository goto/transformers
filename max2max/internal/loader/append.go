package loader

import (
	"fmt"
	"log/slog"
)

type appendLoader struct {
	logger *slog.Logger
}

func NewAppendLoader(logger *slog.Logger) *appendLoader {
	return &appendLoader{
		logger: logger,
	}
}

func (l *appendLoader) GetQuery(tableID, query string) string {
	return fmt.Sprintf("INSERT INTO TABLE %s %s", tableID, query)
}
