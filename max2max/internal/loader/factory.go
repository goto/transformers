package loader

import (
	"fmt"
	"log/slog"
)

type Loader interface {
	GetQuery(tableID, query string) string
	GetPartitionedQuery(tableID, query string, partitionName []string) string
}

func GetLoader(name string, logger *slog.Logger) (Loader, error) {
	switch name {
	case APPEND:
		return NewAppendLoader(logger)
	// case REPLACE:
	// 	return NewReplaceLoader(logger), nil
	// case REPLACE_ALL:
	// 	return NewReplaceAllLoader(logger), nil
	// case MERGE:
	// 	return NewMergeLoader(logger), nil
	// case MERGE_REPLACE:
	// 	return NewMergeReplaceLoader(logger), nil
	default:
		return nil, fmt.Errorf("loader %s not found", name)
	}
}
