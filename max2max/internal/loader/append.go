package loader

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type appendLoader struct {
	logger       *slog.Logger
	meter        metric.Meter
	queryCounter metric.Int64Counter
}

func NewAppendLoader(logger *slog.Logger) (*appendLoader, error) {
	meter := otel.Meter("loader")
	queryCounter, err := meter.Int64Counter("query.count")
	if err != nil {
		return nil, err
	}

	return &appendLoader{
		logger:       logger,
		meter:        meter,
		queryCounter: queryCounter,
	}, nil
}

func (l *appendLoader) GetQuery(tableID, query string) string {
	l.queryCounter.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("method", "GetQuery"),
		attribute.String("tableID", tableID),
	))
	return fmt.Sprintf("INSERT INTO TABLE %s %s", tableID, query)
}

func (l *appendLoader) GetPartitionedQuery(tableID, query string, partitionNames []string) string {
	l.queryCounter.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("method", "GetPartitionedQuery"),
		attribute.String("tableID", tableID),
	))
	return fmt.Sprintf("INSERT INTO TABLE %s PARTITION (%s) %s", tableID, strings.Join(partitionNames, ", "), query)
}
