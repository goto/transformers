package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
	"github.com/goto/transformers/max2max/internal/client"
	"github.com/goto/transformers/max2max/internal/config"
)

// TODO:
// - graceful shutdown
// - error handling
func main() {
	// load config
	cfg, err := config.NewConfig()
	if err != nil {
		panic(err)
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
		panic(err)
	}
	defer client.Close()

	// execute query
	err = client.Execute(ctx, cfg.DestinationTableID, cfg.QueryFilePath)
	if err != nil {
		panic(err)
	}
}
