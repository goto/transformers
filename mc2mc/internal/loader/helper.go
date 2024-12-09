package loader

import (
	"fmt"
	"strings"
)

func SeparateHeadersAndQuery(query string) (string, string) {
	parts := strings.Split(query, ";")

	last := ""
	idx := len(parts) - 1
	for idx >= 0 {
		last = parts[idx]
		if strings.TrimSpace(last) != "" {
			break
		}
		idx = idx - 1
	}

	headers := strings.Join(parts[:idx], ";")
	if headers != "" {
		headers += ";"
	}
	return headers, last
}

func ConstructQueryWithOrderedColumns(query string, orderedColumns []string) string {
	return fmt.Sprintf("SELECT %s FROM (%s)", strings.Join(orderedColumns, ", "), query)
}
