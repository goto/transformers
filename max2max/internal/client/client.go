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

type OdpsClient interface {
	GetPartitionNames(tableID string) ([]string, error)
	ExecSQL(query string) error
}

type Client struct {
	logger     *slog.Logger
	OdpsClient OdpsClient
}

func NewClient(logger *slog.Logger, odpsClient *odps.Odps) *Client {
	return &Client{
		logger:     logger,
		OdpsClient: NewODPSClient(odpsClient),
	}
}

func (c *Client) Execute(loader Loader, tableID, queryFilePath string) error {
	// read query from filepath
	c.logger.Info(fmt.Sprintf("executing query from %s", queryFilePath))
	queryRaw, err := os.ReadFile(queryFilePath)
	if err != nil {
		return err
	}

	// check if table is partitioned
	c.logger.Info(fmt.Sprintf("checking if table %s is partitioned", tableID))
	partitionNames, err := c.OdpsClient.GetPartitionNames(tableID)
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
	if err := c.OdpsClient.ExecSQL(queryToExec); err != nil {
		return err
	}

	c.logger.Info("execution done")
	return nil
}

func (c *Client) Close() {
	// any cleanup
}
