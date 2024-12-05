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
	t.Run("should return error when getting partition name fails", func(t *testing.T) {
		// arrange
		client, err := client.NewClient(context.TODO(), client.SetupLogger("error"))
		require.NoError(t, err)
		client.OdpsClient = &mockOdpsClient{
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
		client, err := client.NewClient(context.TODO(), client.SetupLogger("error"), client.SetupLoader("APPEND"))
		require.NoError(t, err)
		client.OdpsClient = &mockOdpsClient{
			partitionResult: func() ([]string, error) {
				return []string{"event_date"}, nil
			},
			execSQLResult: func() error {
				return nil
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
}

func (m *mockOdpsClient) GetPartitionNames(ctx context.Context, tableID string) ([]string, error) {
	return m.partitionResult()
}

func (m *mockOdpsClient) ExecSQL(ctx context.Context, query string) error {
	return m.execSQLResult()
}
