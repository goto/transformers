package client

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

type Loader interface {
	GetQuery(tableID, query string) string
	GetPartitionedQuery(tableID, query string, partitionName []string) string
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
	partitionNames, err := c.getPartitionNames(tableID)
	if err != nil {
		return err
	}

	// prepare query
	queryToExec := loader.GetQuery(tableID, string(queryRaw))
	if len(partitionNames) > 0 {
		c.logger.Info(fmt.Sprintf("table %s is partitioned by %s", tableID, strings.Join(partitionNames, ", ")))
		queryToExec = loader.GetPartitionedQuery(tableID, string(queryRaw), partitionNames)
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

func (c *client) getPartitionNames(tableID string) ([]string, error) {
	table := c.odpsClient.Table(tableID)
	if err := table.Load(); err != nil {
		return nil, err
	}
	var partitionNames []string
	for _, partition := range table.Schema().PartitionColumns {
		partitionNames = append(partitionNames, partition.Name)
	}
	return partitionNames, nil
}
