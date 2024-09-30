package main

import (
	_ "github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
	"github.com/goto/maxcompute-transformation/internal/client"
	"github.com/goto/maxcompute-transformation/internal/config"
	"github.com/goto/maxcompute-transformation/internal/loader"
	"github.com/goto/maxcompute-transformation/internal/logger"
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
	client := client.NewClient(logger, cfg.GenOdps())
	defer client.Close()

	// execute query
	err = client.Execute(loader, cfg.DestinationTableID, cfg.QueryFilePath)
	if err != nil {
		panic(err)
	}
}
