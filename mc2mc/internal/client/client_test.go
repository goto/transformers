package client_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goto/transformers/mc2mc/internal/client"
)

func TestExecute(t *testing.T) {
	t.Run("should return error when reading query file fails", func(t *testing.T) {
		// arrange
		client, err := client.NewClient(context.TODO(), client.SetupLogger("error"))
		require.NoError(t, err)
		client.OdpsClient = &mockOdpsClient{}
		// act
		err = client.Execute(context.TODO(), "", "./nonexistentfile")
		// assert
		assert.Error(t, err)
	})
	t.Run("should return error when getting ordered columns fails", func(t *testing.T) {
		// arrange
		client, err := client.NewClient(context.TODO(), client.SetupLogger("error"))
		require.NoError(t, err)
		client.OdpsClient = &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return nil, fmt.Errorf("error get ordered columns")
			},
		}
		assert.NoError(t, os.WriteFile("/tmp/query.sql", []byte("SELECT * FROM table;"), 0644))
		// act
		err = client.Execute(context.TODO(), "project_test.table_test", "/tmp/query.sql")
		// assert
		assert.Error(t, err)
		assert.ErrorContains(t, err, "error get ordered columns")
	})
	t.Run("should return error when getting partition name fails", func(t *testing.T) {
		// arrange
		client, err := client.NewClient(context.TODO(), client.SetupLogger("error"))
		require.NoError(t, err)
		client.OdpsClient = &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return []string{"col1", "col2"}, nil
			},
			partitionResult: func() ([]string, error) {
				return nil, fmt.Errorf("error get partition name")
			},
		}
		assert.NoError(t, os.WriteFile("/tmp/query.sql", []byte("SELECT * FROM table;"), 0644))
		// act
		err = client.Execute(context.TODO(), "project_test.table_test", "/tmp/query.sql")
		// assert
		assert.Error(t, err)
		assert.ErrorContains(t, err, "error get partition name")
	})
	t.Run("should return error when executing query fails", func(t *testing.T) {
		// arrange
		client, err := client.NewClient(context.TODO(), client.SetupLogger("error"), client.SetupLoader("APPEND"))
		require.NoError(t, err)
		client.OdpsClient = &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return []string{"col1", "col2"}, nil
			},
			partitionResult: func() ([]string, error) {
				return nil, nil
			},
			execSQLResult: func() error {
				return fmt.Errorf("error exec sql")
			},
		}
		require.NoError(t, os.WriteFile("/tmp/query.sql", []byte("SELECT * FROM table;"), 0644))
		// act
		err = client.Execute(context.TODO(), "project_test.table_test", "/tmp/query.sql")
		// assert
		assert.Error(t, err)
		assert.ErrorContains(t, err, "error exec sql")
	})
	t.Run("should return nil when everything is successful", func(t *testing.T) {
		// arrange
		client, err := client.NewClient(context.TODO(), client.SetupLogger("error"), client.SetupLoader("REPLACE"))
		require.NoError(t, err)
		client.OdpsClient = &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return []string{"col1", "col2"}, nil
			},
			partitionResult: func() ([]string, error) {
				return []string{"event_date"}, nil
			},
			execSQLResult: func() error {
				return nil
			},
		}
		client.Loader = &mockLoader{
			getQueryFunc: func(tableID, query string) string {
				return "INSERT OVERWRITE TABLE project_test.table_test SELECT col1, col2 FROM (SELECT * FROM table);"
			},
			getPartitionedQueryFunc: func(tableID, query string, partitionNames []string) string {
				assert.True(t, true, "should be called")
				return "INSERT OVERWRITE TABLE project_test.table_test PARTITION(event_date) SELECT col1, col2 FROM (SELECT * FROM table);"
			},
		}
		require.NoError(t, os.WriteFile("/tmp/query.sql", []byte("SELECT * FROM table;"), 0644))
		// act
		err = client.Execute(context.TODO(), "project_test.table_test", "/tmp/query.sql")
		// assert
		assert.NoError(t, err)
	})
	t.Run("should return nil when everything is successful with enable auto partition", func(t *testing.T) {
		// arrange
		client, err := client.NewClient(context.TODO(), client.SetupLogger("error"), client.SetupLoader("REPLACE"), client.EnableAutoPartition(true))
		require.NoError(t, err)
		client.OdpsClient = &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return []string{"col1", "col2"}, nil
			},
			partitionResult: func() ([]string, error) {
				return []string{"_partition_value"}, nil
			},
			execSQLResult: func() error {
				return nil
			},
		}
		client.Loader = &mockLoader{
			getQueryFunc: func(tableID, query string) string {
				return "INSERT OVERWRITE TABLE project_test.table_test SELECT col1, col2 FROM (SELECT * FROM table);"
			},
			getPartitionedQueryFunc: func(tableID, query string, partitionNames []string) string {
				assert.False(t, true, "should not be called")
				return "INSERT OVERWRITE TABLE project_test.table_test PARTITION(_partition_value) SELECT col1, col2 FROM (SELECT * FROM table);"
			},
		}
		require.NoError(t, os.WriteFile("/tmp/query.sql", []byte("SELECT * FROM table;"), 0644))
		// act
		err = client.Execute(context.TODO(), "project_test.table_test", "/tmp/query.sql")
		// assert
		assert.NoError(t, err)
	})
}

type mockOdpsClient struct {
	partitionResult func() ([]string, error)
	execSQLResult   func() error
	orderedColumns  func() ([]string, error)
}

func (m *mockOdpsClient) GetPartitionNames(ctx context.Context, tableID string) ([]string, error) {
	return m.partitionResult()
}

func (m *mockOdpsClient) ExecSQL(ctx context.Context, query string) error {
	return m.execSQLResult()
}

func (m *mockOdpsClient) GetOrderedColumns(tableID string) ([]string, error) {
	return m.orderedColumns()
}

type mockLoader struct {
	getQueryFunc            func(tableID, query string) string
	getPartitionedQueryFunc func(tableID, query string, partitionNames []string) string
}

func (m *mockLoader) GetQuery(tableID, query string) string {
	return m.getQueryFunc(tableID, query)
}

func (m *mockLoader) GetPartitionedQuery(tableID, query string, partitionNames []string) string {
	return m.getPartitionedQueryFunc(tableID, query, partitionNames)
}
