package client

import (
	"context"
	e "errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/pkg/errors"
)

type Loader interface {
	GetQuery(tableID, query string) string
	GetPartitionedQuery(tableID, query string, partitionName []string) string
}

type OdpsClient interface {
	GetPartitionNames(ctx context.Context, tableID string) ([]string, error)
	ExecSQL(ctx context.Context, query string) error
}

type Client struct {
	OdpsClient OdpsClient
	Loader     Loader

	appCtx      context.Context
	logger      *slog.Logger
	shutdownFns []func() error

	// TODO: remove this temporary capability after 15 nov
	enablePartitionValue bool
}

func NewClient(ctx context.Context, setupFns ...SetupFn) (*Client, error) {
	c := &Client{
		appCtx:      ctx,
		shutdownFns: make([]func() error, 0),
	}
	for _, setupFn := range setupFns {
		if err := setupFn(c); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return c, nil
}

func (c *Client) Close() error {
	c.logger.Info("closing client")
	var err error
	for _, fn := range c.shutdownFns {
		err = e.Join(err, fn())
	}
	return errors.WithStack(err)
}

func (c *Client) Execute(ctx context.Context, tableID, queryFilePath string) error {
	// read query from filepath
	c.logger.Info(fmt.Sprintf("executing query from %s", queryFilePath))
	queryRaw, err := os.ReadFile(queryFilePath)
	if err != nil {
		return errors.WithStack(err)
	}
	if c.enablePartitionValue {
		queryRaw = addPartitionValueColumn(queryRaw)
	}

	// check if table is partitioned
	partitionNames, err := c.OdpsClient.GetPartitionNames(ctx, tableID)
	if err != nil {
		return errors.WithStack(err)
	}

	// prepare query
	queryToExec := c.Loader.GetQuery(tableID, string(queryRaw))
	if len(partitionNames) > 0 {
		c.logger.Info(fmt.Sprintf("table %s is partitioned by %s", tableID, strings.Join(partitionNames, ", ")))
		queryToExec = c.Loader.GetPartitionedQuery(tableID, string(queryRaw), partitionNames)
	}

	// execute query with odps client
	c.logger.Info(fmt.Sprintf("execute: %s", queryToExec))
	if err := c.OdpsClient.ExecSQL(ctx, queryToExec); err != nil {
		return errors.WithStack(err)
	}

	c.logger.Info("execution done")
	return errors.WithStack(err)
}

// TODO: remove this temporary support after 15 nov
func addPartitionValueColumn(rawQuery []byte) []byte {
	return []byte(fmt.Sprintf("SELECT *, STRING(CURRENT_DATE()) as __partitionvalue FROM (%s);", rawQuery))
}
