package loader

import (
	"fmt"
	"log/slog"

	"github.com/pkg/errors"
)

type Loader interface {
	GetQuery(tableID, query string) string
	GetPartitionedQuery(tableID, query string, partitionName []string) string
}

func GetLoader(name string, logger *slog.Logger) (Loader, error) {
	switch name {
	case APPEND:
		return NewAppendLoader(logger)
	case REPLACE:
		return NewReplaceLoader(logger)
	// case REPLACE_ALL:
	// 	return NewReplaceAllLoader(logger), nil
	// case MERGE:
	// 	return NewMergeLoader(logger), nil
	// case MERGE_REPLACE:
	// 	return NewMergeReplaceLoader(logger), nil
	default:
		err := fmt.Errorf("loader %s not found", name)
		return nil, errors.WithStack(err)
	}
}
