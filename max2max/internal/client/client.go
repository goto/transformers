package client

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
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
	OdpsClient OdpsClient

	logger      *slog.Logger
	shutdownFns []func() error
}

func NewClient(setupFns ...SetupFn) (*Client, error) {
	c := &Client{
		shutdownFns: make([]func() error, 0),
	}
	for _, setupFn := range setupFns {
		if err := setupFn(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *Client) Close() error {
	var err error
	for _, fn := range c.shutdownFns {
		err = errors.Join(err, fn())
	}
	return err
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
