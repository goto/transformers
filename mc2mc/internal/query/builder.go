package query

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/pkg/errors"
)

// Interface from odps client to support query builder
type OdpsClient interface {
	GetOrderedColumns(tableID string) ([]string, error)
	GetPartitionNames(ctx context.Context, tableID string) ([]string, error)
}

// Builder is a query builder for constructing final query
type Builder struct {
	l      *slog.Logger
	client OdpsClient

	query string

	method             Method
	destinationTableID string
	orderedColumns     []string
	overridedValues    map[string]string

	enableAutoPartition  bool
	enablePartitionValue bool
}

// NewBuilder creates a new query builder with the given options
func NewBuilder(l *slog.Logger, client OdpsClient, options ...Option) *Builder {
	b := &Builder{
		l:                    l,
		client:               client,
		method:               MERGE, // default method (script)
		destinationTableID:   "",
		orderedColumns:       nil,
		overridedValues:      nil,
		enableAutoPartition:  false,
		enablePartitionValue: false,
	}
	return b.SetOptions(options...)
}

// SetOption sets the given option to the builder
func (b *Builder) SetOptions(options ...Option) *Builder {
	for _, opt := range options {
		opt(b)
	}
	return b
}

// Build constructs the final query with the given options
func (b *Builder) Build() (string, error) {
	if b.query == "" {
		return "", errors.New("query is required")
	}

	if b.method == MERGE {
		// split query components
		hrs, vars, queries := SplitQueryComponents(b.query)
		if len(queries) <= 1 {
			return b.query, nil
		}
		query := b.constructMergeQuery(hrs, vars, queries)
		return query, nil
	}

	// separate headers, variables and udfs from the query
	hr, query := SeparateHeadersAndQuery(b.query)
	varsAndUDFs, query := SeparateVariablesUDFsAndQuery(query)
	drops, query := SeparateDropsAndQuery(query)

	// destination table is required for append and replace method
	if b.destinationTableID == "" {
		return "", errors.New("destination table is required")
	}

	var err error

	// construct overrided values if enabled
	if b.overridedValues != nil {
		query, err = b.constructOverridedValues(query)
		if err != nil {
			return "", errors.WithStack(err)
		}
	}

	// construct column order
	if b.orderedColumns != nil {
		query, err = b.constructColumnOrder(query)
		if err != nil {
			return "", errors.WithStack(err)
		}
	}

	// construct partition value if enabled
	// this is for temporary solution to support partition value
	// partition value is a pseudo column __partitionvalue,
	// so it's not part of the ordered columns
	if b.enablePartitionValue && !b.enableAutoPartition {
		query, err = b.constructPartitionValue(query)
		if err != nil {
			return "", errors.WithStack(err)
		}
	}

	// fetch partition names
	partitionNames, err := b.client.GetPartitionNames(context.Background(), b.destinationTableID)
	if err != nil {
		return "", errors.WithStack(err)
	}

	// if non partitioned table or auto partition is enabled
	// query can be constructed without explicit partition
	if len(partitionNames) == 0 || b.enableAutoPartition {
		switch b.method {
		case APPEND:
			query = fmt.Sprintf("INSERT INTO TABLE %s \n%s\n;", b.destinationTableID, query)
		case REPLACE:
			query = fmt.Sprintf("INSERT OVERWRITE TABLE %s \n%s\n;", b.destinationTableID, query)
		}
	} else {
		switch b.method {
		case APPEND:
			query = fmt.Sprintf("INSERT INTO TABLE %s PARTITION (%s) \n%s\n;", b.destinationTableID, strings.Join(partitionNames, ", "), query)
		case REPLACE:
			query = fmt.Sprintf("INSERT OVERWRITE TABLE %s PARTITION (%s) \n%s\n;", b.destinationTableID, strings.Join(partitionNames, ", "), query)
		}
	}

	// construct final query with headers, drops, variables and udfs
	if hr != "" {
		hr += "\n"
	}
	if drops != "" {
		drops += "\n"
	}
	if varsAndUDFs != "" {
		varsAndUDFs += "\n"
	}
	query = fmt.Sprintf("%s%s%s%s", hr, drops, varsAndUDFs, query)
	return query, nil
}

// separateHeadersAndQuery separates headers and query from the given query
func (b *Builder) constructColumnOrder(query string) (string, error) {
	if b.orderedColumns == nil || len(b.orderedColumns) == 0 {
		columns, err := b.client.GetOrderedColumns(b.destinationTableID)
		if err != nil {
			b.l.Error(fmt.Sprintf("failed to get ordered columns: %s", err.Error()))
			return "", errors.WithStack(err)
		}
		b.orderedColumns = columns
	}
	return fmt.Sprintf("SELECT %s FROM (\n%s\n)", strings.Join(b.orderedColumns, ", "), query), nil
}

// constructPartitionValue constructs partition value for the given query
// by adding a pseudo column __partitionvalue with the current date
// this is for temporary solution to support partition value
func (b *Builder) constructPartitionValue(query string) (string, error) {
	return fmt.Sprintf("SELECT *, STRING(CURRENT_DATE()) as __partitionvalue FROM (\n%s\n)", query), nil
}

// constructOverridedValues constructs query with overrided values
func (b *Builder) constructOverridedValues(query string) (string, error) {
	if b.orderedColumns == nil || len(b.orderedColumns) == 0 {
		columns, err := b.client.GetOrderedColumns(b.destinationTableID)
		if err != nil {
			b.l.Error(fmt.Sprintf("failed to get ordered columns: %s", err.Error()))
			return "", errors.WithStack(err)
		}
		b.orderedColumns = columns
	}
	columns := make([]string, len(b.orderedColumns))
	for i, col := range b.orderedColumns {
		columns[i] = col
		if val, ok := b.overridedValues[col]; ok {
			columns[i] = fmt.Sprintf("%s as %s", val, col)
		}
	}
	return fmt.Sprintf("SELECT %s FROM (\n%s\n)", strings.Join(columns, ", "), query), nil
}

// constructMergeQueries constructs merge queries with headers and variables
func (b *Builder) constructMergeQuery(hrs, vars, queries []string) string {
	builder := strings.Builder{}
	for i, q := range queries {
		q = strings.TrimSpace(q)
		if q == "" || strings.TrimSpace(RemoveComments(q)) == "" {
			continue
		}
		headers := JoinSliceString(hrs[:i+1], "\n")
		variables := JoinSliceString(vars[:i+1], "\n")
		if headers != "" {
			builder.WriteString(fmt.Sprintf("%s\n", headers))
		}
		if variables != "" {
			builder.WriteString(fmt.Sprintf("%s\n", variables))
		}
		builder.WriteString(fmt.Sprintf("%s\n;", q))
		if i < len(queries)-1 {
			builder.WriteString(fmt.Sprintf("\n%s\n", BREAK_MARKER))
		}
	}
	return builder.String()
}
