package client

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

type Loader interface {
	GetQuery(tableID, query string) string
}

type client struct {
	logger  *slog.Logger
	odpsIns *odps.Odps
}

func NewClient(logger *slog.Logger, odpsIns *odps.Odps) *client {
	return &client{
		logger:  logger,
		odpsIns: odpsIns,
	}
}

func (c *client) Execute(loader Loader, tableID, queryFilePath string) error {
	// read query from filepath
	c.logger.Info(fmt.Sprintf("executing query from %s", queryFilePath))
	queryRaw, err := os.ReadFile(queryFilePath)
	if err != nil {
		return err
	}

	// execute query with odps client
	c.logger.Info(fmt.Sprintf("execute: %s", string(queryRaw)))
	ins, err := c.odpsIns.ExecSQl(loader.GetQuery(tableID, string(queryRaw)))
	if err != nil {
		return err
	}
	c.logger.Info(fmt.Sprintf("taskId: %s", ins.Id()))

	// wait execution success
	if err := ins.WaitForSuccess(); err != nil {
		return err
	}
	c.logger.Info("execution done")
	return nil
}
