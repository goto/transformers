package client

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

func setupOTelSDK(collectorGRPCEndpoint string) (shutdown func() error, err error) {
	ctx := context.Background() // TODO: use context from main
	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpoint(collectorGRPCEndpoint))
	if err != nil {
		return nil, err
	}

	// for now, we only need metric provider
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(resource.Default()), // TODO: add resource specific to job name and plugin name
		metric.WithReader(metric.NewPeriodicReader(metricExporter)),
	)
	otel.SetMeterProvider(meterProvider)

	return func() error {
		return meterProvider.Shutdown(ctx)
	}, nil
}
