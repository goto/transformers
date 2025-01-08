package query

import (
	"regexp"
	"strings"
)

const (
	BREAK_MARKER = "--*--optimus-break-marker--*--"
)

var (
	headerPattern = regexp.MustCompile(`(?im)^\s*set\s+[^;]+;\s*`) // regex to match header statements
)

func SeparateHeadersAndQuery(query string) (string, string) {
	query = strings.TrimSpace(query)

	// extract all header lines (SET statements and comments)
	headers := headerPattern.FindAllString(query, -1)
	// Remove all headers from the original query to get the remaining query
	remainingQuery := strings.TrimSpace(headerPattern.ReplaceAllString(query, ""))

	headerStr := ""
	if len(headers) > 0 {
		for i, header := range headers {
			headers[i] = strings.TrimSpace(header)
		}
		headerStr = strings.Join(headers, "\n")
	}

	// remove any leading semicolons from the remaining SQL
	queryStr := strings.TrimSuffix(remainingQuery, ";")

	// Trim any remaining whitespace from both parts
	return strings.TrimSpace(headerStr), queryStr
}
