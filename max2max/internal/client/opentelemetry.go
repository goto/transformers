package client

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
)

func initOpenTelemetry() (func(context.Context) error, error) {
	ctx := context.Background()

	// Create OTLP trace exporter
	traceExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}

	// Create trace provider
	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
		trace.WithResource(resource.NewWithAttributes(
			attribute.String("service.name", "example-service"),
		)),
	)
	otel.SetTracerProvider(traceProvider)

	// Create metric controller
	metricController := basic.New(
		simple.NewWithExactDistribution(),
		metric.WithResource(resource.NewWithAttributes(
			attribute.String("service.name", "example-service"),
		)),
		metric.WithReader(metric.NewPeriodicReader(
			metric.NewExportPipeline(
				metric.NewSimpleSelector(),
				aggregation.CumulativeTemporalitySelector(),
			),
			time.Minute,
		)),
	)
	global.SetMeterProvider(metricController.MeterProvider())

	return func(ctx context.Context) error {
		if err := traceProvider.Shutdown(ctx); err != nil {
			return err
		}
		return metricController.Stop(ctx)
	}, nil
}
