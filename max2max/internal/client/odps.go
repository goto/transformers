package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

type odpsClient struct {
	logger *slog.Logger
	client *odps.Odps
}

// NewODPSClient creates a new odpsClient instance
func NewODPSClient(logger *slog.Logger, client *odps.Odps) *odpsClient {
	return &odpsClient{
		logger: logger,
		client: client,
	}
}

// ExecSQL executes the given query in syncronous mode (blocking)
// with capability to do graceful shutdown by terminating task instance
// when context is cancelled.
func (c *odpsClient) ExecSQL(ctx context.Context, query string) error {
	taskIns, err := c.client.ExecSQl(query)
	if err != nil {
		return err
	}

	// generate log view
	url, err := odps.NewLogView(c.client).GenerateLogView(taskIns, 1)
	if err != nil {
		return err
	}
	c.logger.Info(fmt.Sprintf("log view: %s", url))

	// wait execution success
	c.logger.Info(fmt.Sprintf("taskId: %s", taskIns.Id()))
	select {
	case <-ctx.Done():
		c.logger.Info("context cancelled, terminating task instance")
		err := taskIns.Terminate()
		return errors.Join(ctx.Err(), err)
	case err := <-wait(taskIns):
		return err
	}
}

// GetPartitionNames returns the partition names of the given table
// by querying the table schema.
func (c *odpsClient) GetPartitionNames(_ context.Context, tableID string) ([]string, error) {
	table := c.client.Table(tableID)
	if err := table.Load(); err != nil {
		return nil, err
	}
	var partitionNames []string
	for _, partition := range table.Schema().PartitionColumns {
		partitionNames = append(partitionNames, partition.Name)
	}
	return partitionNames, nil
}

// wait waits for the task instance to finish on a separate goroutine
func wait(taskIns *odps.Instance) <-chan error {
	errChan := make(chan error)
	go func(errChan chan<- error) {
		defer close(errChan)
		errChan <- taskIns.WaitForSuccess()
	}(errChan)
	return errChan
}
