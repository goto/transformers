package client

import (
	"fmt"
	"log/slog"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

type odpsClient struct {
	logger *slog.Logger
	client *odps.Odps
}

func NewODPSClient(client *odps.Odps) *odpsClient {
	return &odpsClient{
		client: client,
	}
}

// ExecSQL executes the given query in syncronous mode (blocking)
// TODO: change the execution mode to async and do graceful shutdown
func (c *odpsClient) ExecSQL(query string) error {
	taskIns, err := c.client.ExecSQl(query)
	if err != nil {
		return err
	}

	// wait execution success
	c.logger.Info(fmt.Sprintf("taskId: %s", taskIns.Id()))
	return taskIns.WaitForSuccess()
}

func (c *odpsClient) GetPartitionNames(tableID string) ([]string, error) {
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
