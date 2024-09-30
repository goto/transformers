package main

import (
	_ "github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
	"github.com/goto/transformers/max2max/internal/client"
	"github.com/goto/transformers/max2max/internal/config"
	"github.com/goto/transformers/max2max/internal/loader"
	"github.com/goto/transformers/max2max/internal/logger"
)

// TODO:
// - graceful shutdown
// - error handling
// - instrumentation
func main() {
	// load config
	cfg, err := config.NewConfig()
	if err != nil {
		panic(err)
	}

	// initiate dependencies
	logger, err := logger.NewLogger(cfg.LogLevel)
	if err != nil {
		panic(err)
	}
	loader, err := loader.GetLoader(cfg.LoadMethod, logger)
	if err != nil {
		panic(err)
	}
	// initiate client
	client, err := client.NewClient(
		client.SetupLogger(cfg.LogLevel),
		client.SetupOTelSDK(cfg.OtelCollectorGRPCEndpoint),
		client.SetupODPSClient(cfg.GenOdps()),
	)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// execute query
	err = client.Execute(loader, cfg.DestinationTableID, cfg.QueryFilePath)
	if err != nil {
		panic(err)
	}
}
