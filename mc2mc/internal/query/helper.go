package query

import (
	"regexp"
	"strings"
)

const (
	BREAK_MARKER = "--*--optimus-break-marker--*--"
)

var (
	headerPattern = regexp.MustCompile(`(?i)^\s*set\s+[^;]+;`) // regex to match header statements
)

func SeparateHeadersAndQuery(query string) (string, string) {
	query = strings.TrimSpace(query)

	headers := []string{}
	remainingQuery := query

	// keep matching header statements until there are no more
	for {
		match := headerPattern.FindString(remainingQuery)
		if match == "" {
			break
		}
		headers = append(headers, strings.TrimSpace(match))
		remainingQuery = strings.TrimSpace(remainingQuery[len(match):])
	}

	headerStr := ""
	if len(headers) > 0 {
		headerStr = strings.Join(headers, "\n")
	}

	// remove any leading semicolons from the remaining SQL
	queryStr := strings.TrimSuffix(remainingQuery, ";")

	// Trim any remaining whitespace from both parts
	return strings.TrimSpace(headerStr), queryStr
}
