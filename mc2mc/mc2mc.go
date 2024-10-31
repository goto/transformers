package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/goto/transformers/mc2mc/internal/client"
	"github.com/goto/transformers/mc2mc/internal/config"
	"github.com/pkg/errors"
)

func mc2mc() error {
	// load config
	cfg, err := config.NewConfig()
	if err != nil {
		return errors.WithStack(err)
	}

	// graceful shutdown
	ctx, cancelFn := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelFn()

	// initiate client
	client, err := client.NewClient(
		ctx,
		client.SetupLogger(cfg.LogLevel),
		client.SetupOTelSDK(cfg.OtelCollectorGRPCEndpoint, cfg.JobName, cfg.ScheduledTime),
		client.SetupODPSClient(cfg.GenOdps()),
		client.SetupLoader(cfg.LoadMethod),
	)
	if err != nil {
		return errors.WithStack(err)
	}
	defer client.Close()

	// execute query
	err = client.Execute(ctx, cfg.DestinationTableID, cfg.QueryFilePath)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
