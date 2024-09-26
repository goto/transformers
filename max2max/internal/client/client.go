package client

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

type Loader interface {
	GetQuery(tableID, query string) string
	GetPartitionedQuery(tableID, partitionName, query string) string
}

type client struct {
	logger     *slog.Logger
	odpsClient *odps.Odps
}

func NewClient(logger *slog.Logger, odpsClient *odps.Odps) *client {
	return &client{
		logger:     logger,
		odpsClient: odpsClient,
	}
}

func (c *client) Execute(loader Loader, tableID, queryFilePath string) error {
	// read query from filepath
	c.logger.Info(fmt.Sprintf("executing query from %s", queryFilePath))
	queryRaw, err := os.ReadFile(queryFilePath)
	if err != nil {
		return err
	}

	// check if table is partitioned
	c.logger.Info(fmt.Sprintf("checking if table %s is partitioned", tableID))
	partitionName, err := c.getPartitionName(tableID)
	if err != nil {
		return err
	}

	// prepare query
	queryToExec := loader.GetQuery(tableID, string(queryRaw))
	if partitionName != "" {
		c.logger.Info(fmt.Sprintf("table %s is partitioned by %s", tableID, partitionName))
		queryToExec = loader.GetPartitionedQuery(tableID, partitionName, string(queryRaw))
	}

	// execute query with odps client
	c.logger.Info(fmt.Sprintf("execute: %s", queryToExec))
	taskIns, err := c.odpsClient.ExecSQl(queryToExec)
	if err != nil {
		return err
	}

	// wait execution success
	c.logger.Info(fmt.Sprintf("taskId: %s", taskIns.Id()))
	if err := taskIns.WaitForSuccess(); err != nil {
		return err
	}
	c.logger.Info("execution done")
	return nil
}

func (c *client) getPartitionName(tableID string) (string, error) {
	table := c.odpsClient.Table(tableID)
	if err := table.Load(); err != nil {
		return "", err
	}
	if len(table.Schema().PartitionColumns) > 0 {
		return table.Schema().PartitionColumns[0].Name, nil
	}
	return "", nil
}
