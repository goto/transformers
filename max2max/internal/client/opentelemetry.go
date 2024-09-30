package client

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

func setupOTelSDK(collectorGRPCEndpoint string, jobName, scheduledTime string) (shutdown func() error, err error) {
	ctx := context.Background() // TODO: use context from main
	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(collectorGRPCEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	// for now, we only need metric provider
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(resource.NewWithAttributes(
			resource.Default().SchemaURL(),
			attribute.String("plugin.name", "max2max"),
			attribute.String("job.name", jobName),
			attribute.String("job.scheduled_time", scheduledTime),
		)),
		metric.WithReader(metric.NewPeriodicReader(metricExporter)),
	)
	otel.SetMeterProvider(meterProvider)

	return func() error {
		return meterProvider.Shutdown(ctx)
	}, nil
}
