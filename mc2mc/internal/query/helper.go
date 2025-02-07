package query

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	BREAK_MARKER = "--*--optimus-break-marker--*--"
)

var (
	semicolonPattern    = regexp.MustCompile(`;\s*(\n+|$)`)      // regex to match semicolons
	commentPattern      = regexp.MustCompile(`--[^\n]*`)         // regex to match comments
	multiCommentPattern = regexp.MustCompile(`(?s)/\*.*?\*/`)    // regex to match multi-line comments
	headerPattern       = regexp.MustCompile(`(?i)^set`)         // regex to match header statements
	variablePattern     = regexp.MustCompile(`(?i)^@`)           // regex to match variable statements
	udfPattern          = regexp.MustCompile(`(?i)^function\s+`) // regex to match UDF statements
	ddlPattern          = regexp.MustCompile(`(?i)^CREATE\s+`)   // regex to match DDL statements
	stringPattern       = regexp.MustCompile(`'[^']*'`)          // regex to match SQL strings (anything inside single quotes)
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
		stmtWithoutComment := commentPattern.ReplaceAllString(stmt, "")
		if headerPattern.MatchString(strings.TrimSpace(stmtWithoutComment)) {
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

func SeparateVariablesUDFsAndQuery(query string) (string, string) {
	variablesAndUDFs := []string{}
	query = strings.TrimSpace(query)
	remainingQueries := []string{}

	// extract all variable lines (@ statements and comments)
	stmts := semicolonPattern.Split(query, -1)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmtWithoutComment := commentPattern.ReplaceAllString(stmt, "")
		if variablePattern.MatchString(strings.TrimSpace(stmtWithoutComment)) ||
			udfPattern.MatchString(strings.TrimSpace(stmtWithoutComment)) {
			variablesAndUDFs = append(variablesAndUDFs, stmt)
		} else {
			remainingQueries = append(remainingQueries, stmt)
		}
	}

	variableUDFStr := ""
	if len(variablesAndUDFs) > 0 {
		for i, variable := range variablesAndUDFs {
			variablesAndUDFs[i] = strings.TrimSpace(variable)
		}
		variableUDFStr = strings.Join(variablesAndUDFs, ";\n")
		variableUDFStr += ";"
	}

	// join the remaining queries back together
	queryStr := strings.Join(remainingQueries, ";\n")

	return variableUDFStr, queryStr
}

func RemoveComments(query string) string {
	query = commentPattern.ReplaceAllString(query, "")
	query = multiCommentPattern.ReplaceAllString(query, "")
	return query
}

func ProtectedStringLiteral(query string) (map[string]string, string) {
	// Replace all strings with a placeholder to protect them
	placeholders := make(map[string]string)
	protectedQuery := stringPattern.ReplaceAllStringFunc(query, func(match string) string {
		placeholder := fmt.Sprintf("__STRING_PLACEHOLDER_%d__", len(placeholders))
		placeholders[placeholder] = match
		return placeholder
	})
	return placeholders, protectedQuery
}

func RestoreStringLiteral(query string, placeholders map[string]string) string {
	// Restore all strings from the placeholders
	for placeholder, original := range placeholders {
		query = strings.ReplaceAll(query, placeholder, original)
	}
	return query
}

func IsDDL(query string) bool {
	return ddlPattern.MatchString(query)
}
