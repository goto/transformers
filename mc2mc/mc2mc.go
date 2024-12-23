package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"

	"github.com/goto/transformers/mc2mc/internal/client"
	"github.com/goto/transformers/mc2mc/internal/config"
	"github.com/goto/transformers/mc2mc/internal/logger"
	"github.com/goto/transformers/mc2mc/internal/query"
)

func mc2mc(envs []string) error {
	// load config
	cfg, err := config.NewConfig(envs...)
	if err != nil {
		return errors.WithStack(err)
	}

	// set up logger
	l, err := logger.NewLogger(cfg.LogLevel)
	if err != nil {
		return errors.WithStack(err)
	}

	// graceful shutdown
	ctx, cancelFn := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelFn()

	// initiate client
	c, err := client.NewClient(
		ctx,
		client.SetupLogger(l),
		client.SetupOTelSDK(cfg.OtelCollectorGRPCEndpoint, cfg.OtelCollectorGRPCEndpoint),
		client.SetupODPSClient(cfg.GenOdps()),
	)
	if err != nil {
		return errors.WithStack(err)
	}
	defer c.Close()

	// normalize date as a temporary support
	start, err := time.Parse(time.RFC3339, cfg.DStart)
	if err != nil {
		return errors.WithStack(err)
	}
	dstart := start.Format(time.DateTime)

	// build query
	raw, err := os.ReadFile(cfg.QueryFilePath)
	if err != nil {
		return errors.WithStack(err)
	}
	queryToExecute := string(raw)
	switch cfg.LoadMethod {
	case "APPEND":
		queryToExecute, err = query.NewBuilder(
			l,
			client.NewODPSClient(l, cfg.GenOdps()),
			queryToExecute,
			query.WithMethod(query.APPEND),
			query.WithDestination(cfg.DestinationTableID),
			query.WithOverridedValue("_partitiontime", fmt.Sprintf("TIMESTAMP('%s')", dstart)),
			query.WithOverridedValue("_partitiondate", fmt.Sprintf("DATE(TIMESTAMP('%s'))", dstart)),
			query.WithAutoPartition(cfg.DevEnableAutoPartition == "true"),
			query.WithPartitionValue(cfg.DevEnablePartitionValue == "true"),
			query.WithColumnOrder(),
		).Build()
	case "REPLACE":
		queryToExecute, err = query.NewBuilder(
			l,
			client.NewODPSClient(l, cfg.GenOdps()),
			queryToExecute,
			query.WithMethod(query.REPLACE),
			query.WithDestination(cfg.DestinationTableID),
			query.WithOverridedValue("_partitiontime", fmt.Sprintf("TIMESTAMP('%s')", dstart)),
			query.WithOverridedValue("_partitiondate", fmt.Sprintf("DATE(TIMESTAMP('%s'))", dstart)),
			query.WithAutoPartition(cfg.DevEnableAutoPartition == "true"),
			query.WithPartitionValue(cfg.DevEnablePartitionValue == "true"),
			query.WithColumnOrder(),
		).Build()
	case "MERGE":
		queryToExecute, err = query.NewBuilder(
			l,
			client.NewODPSClient(l, cfg.GenOdps()),
			queryToExecute,
			query.WithMethod(query.MERGE),
		).Build()
	default:
		return errors.Errorf("not supported load method: %s", cfg.LoadMethod)
	}
	if err != nil {
		return errors.WithStack(err)
	}

	// execute query
	err = c.Execute(ctx, queryToExecute)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
