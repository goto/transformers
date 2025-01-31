package query

import (
	"regexp"
	"strings"
)

const (
	BREAK_MARKER = "--*--optimus-break-marker--*--"
)

var (
	semicolonPattern = regexp.MustCompile(`;(\n+|$)`)        // regex to match semicolons
	commentRegex     = regexp.MustCompile(`(?m)^\s*--.*\n?`) // regex to match comments
	headerPattern    = regexp.MustCompile(`(?i)^set`)        // regex to match header statements
	variablePattern  = regexp.MustCompile(`(?i)^@`)          // regex to match variable statements
	ddlPattern       = regexp.MustCompile(`(?i)^CREATE\s+`)  // regex to match DDL statements
)

func SeparateHeadersAndQuery(query string) (string, string) {
	headers := []string{}
	query = strings.TrimSpace(query)
	remainingQueries := []string{}

	// extract all header lines (set statements)
	stmts := semicolonPattern.Split(query, -1)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmtWithoutComment := commentRegex.ReplaceAllString(stmt, "")
		if headerPattern.MatchString(stmtWithoutComment) {
			headers = append(headers, stmt)
		} else {
			remainingQueries = append(remainingQueries, stmt)
		}
	}

	headerStr := ""
	if len(headers) > 0 {
		for i, header := range headers {
			headers[i] = strings.TrimSpace(header)
		}
		headerStr = strings.Join(headers, ";\n")
		headerStr += ";"
	}

	// join the remaining queries back together
	queryStr := strings.Join(remainingQueries, ";\n")

	return headerStr, queryStr
}

func SeparateVariablesAndQuery(query string) (string, string) {
	variables := []string{}
	query = strings.TrimSpace(query)
	remainingQueries := []string{}

	// extract all variable lines (@ statements and comments)
	stmts := semicolonPattern.Split(query, -1)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmtWithoutComment := commentRegex.ReplaceAllString(stmt, "")
		if variablePattern.MatchString(stmtWithoutComment) {
			variables = append(variables, stmt)
		} else {
			remainingQueries = append(remainingQueries, stmt)
		}
	}

	variableStr := ""
	if len(variables) > 0 {
		for i, variable := range variables {
			variables[i] = strings.TrimSpace(variable)
		}
		variableStr = strings.Join(variables, ";\n")
		variableStr += ";"
	}

	// join the remaining queries back together
	queryStr := strings.Join(remainingQueries, ";\n")

	return variableStr, queryStr
}

func IsDDL(query string) bool {
	return ddlPattern.MatchString(query)
}
