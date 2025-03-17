package client

import (
	"context"
	e "errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/pkg/errors"
)

type odpsClient struct {
	logger *slog.Logger
	client *odps.Odps

	logViewRetentionInDays int
	additionalHints        map[string]string
}

// NewODPSClient creates a new odpsClient instance
func NewODPSClient(logger *slog.Logger, client *odps.Odps) *odpsClient {
	return &odpsClient{
		logger:                 logger,
		client:                 client,
		logViewRetentionInDays: 2,
	}
}

// ExecSQL executes the given query in syncronous mode (blocking)
// with capability to do graceful shutdown by terminating task instance
// when context is cancelled.
func (c *odpsClient) ExecSQL(ctx context.Context, query string) error {
	hints := addHints(c.additionalHints, query)
	taskIns, err := c.client.ExecSQlWithHints(query, hints)
	if err != nil {
		return errors.WithStack(err)
	}

	// generate log view
	url, err := odps.NewLogView(c.client).GenerateLogView(taskIns, c.logViewRetentionInDays*24)
	if err != nil {
		err = e.Join(err, taskIns.Terminate())
		return errors.WithStack(err)
	}
	c.logger.Info(fmt.Sprintf("log view: %s", url))

	// wait execution success
	c.logger.Info(fmt.Sprintf("taskId: %s", taskIns.Id()))
	select {
	case <-ctx.Done():
		c.logger.Info("context cancelled, terminating task instance")
		err := taskIns.Terminate()
		return e.Join(ctx.Err(), err)
	case err := <-wait(taskIns):
		return errors.WithStack(err)
	}
}

// SetAdditionalHints sets the additional hints for the odps client
func (c *odpsClient) SetAdditionalHints(hints map[string]string) {
	c.additionalHints = hints
}

// SetLogViewRetentionInDays sets the log view retention in days
func (c *odpsClient) SetLogViewRetentionInDays(days int) {
	c.logViewRetentionInDays = days
}

// SetDefaultProject sets the default project of the odps client
func (c *odpsClient) SetDefaultProject(project string) {
	c.client.SetDefaultProjectName(project)
}

// GetPartitionNames returns the partition names of the given table
// by querying the table schema.
func (c *odpsClient) GetPartitionNames(_ context.Context, tableID string) ([]string, error) {
	table, err := getTable(c.client, tableID)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var partitionNames []string
	for _, partition := range table.Schema().PartitionColumns {
		partitionNames = append(partitionNames, partition.Name)
	}

	return partitionNames, nil
}

// GetOrderedColumns returns the ordered column names of the given table
// by querying the table schema.
func (c *odpsClient) GetOrderedColumns(tableID string) ([]string, error) {
	table, err := getTable(c.client, tableID)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var columnNames []string
	for _, column := range table.Schema().Columns {
		columnNames = append(columnNames, column.Name)
	}

	return columnNames, nil
}

// wait waits for the task instance to finish on a separate goroutine
func wait(taskIns *odps.Instance) <-chan error {
	errChan := make(chan error)
	go func(errChan chan<- error) {
		defer close(errChan)
		err := taskIns.WaitForSuccess()
		errChan <- errors.WithStack(err)
	}(errChan)
	return errChan
}

func addHints(additionalHints map[string]string, query string) map[string]string {
	hints := make(map[string]string)
	for k, v := range additionalHints {
		hints[k] = v
	}
	multisql := strings.Contains(query, ";")
	if multisql {
		hints["odps.sql.submit.mode"] = "script"
	}

	return hints
}

// getTable returns the table with the given tableID
func getTable(client *odps.Odps, tableID string) (*odps.Table, error) {
	// save current project and schema
	currProject := client.DefaultProjectName()
	currSchema := client.CurrentSchemaName()
	defer func() {
		// restore current project and schema
		client.SetDefaultProjectName(currProject)
		client.SetCurrentSchemaName(currSchema)
	}()

	splittedTableID := strings.Split(tableID, ".")
	if len(splittedTableID) != 3 {
		return nil, errors.Errorf("invalid tableID (tableID should be in format project.schema.table): %s", tableID)
	}
	project, schema, name := splittedTableID[0], splittedTableID[1], splittedTableID[2]

	// set project and schema to the table
	client.SetDefaultProjectName(project)
	client.SetCurrentSchemaName(schema)

	// get table
	table := client.Tables().Get(name)
	if err := table.Load(); err != nil {
		return nil, errors.WithStack(err)
	}
	return table, nil
}
