package client_test

import (
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/goto/maxcompute-transformation/internal/client"
	"github.com/stretchr/testify/assert"
)

func TestExecute(t *testing.T) {
	t.Run("should return error when reading query file fails", func(t *testing.T) {
		// arrange
		client := client.NewClient(slog.Default(), nil)
		client.OdpsClient = &mockOdpsClient{}
		// act
		err := client.Execute(nil, "", "./nonexistentfile")
		// assert
		assert.Error(t, err)
	})
	t.Run("should return error when getting partition name fails", func(t *testing.T) {
		// arrange
		client := client.NewClient(slog.Default(), nil)
		client.OdpsClient = &mockOdpsClient{
			partitionResult: func() ([]string, error) {
				return nil, fmt.Errorf("error get partition name")
			},
		}
		assert.NoError(t, os.WriteFile("/tmp/query.sql", []byte("SELECT * FROM table;"), 0644))
		// act
		err := client.Execute(nil, "project_test.table_test", "/tmp/query.sql")
		// assert
		assert.Error(t, err)
		assert.ErrorContains(t, err, "error get partition name")
	})
	t.Run("should return error when executing query fails", func(t *testing.T) {
		// arrange
		client := client.NewClient(slog.Default(), nil)
		client.OdpsClient = &mockOdpsClient{
			partitionResult: func() ([]string, error) {
				return nil, nil
			},
			execSQLResult: func() error {
				return fmt.Errorf("error exec sql")
			},
		}
		loader := &mockLoader{
			getQueryResult: func() string {
				return "INSERT INTO table SELECT * FROM table;"
			},
		}
		assert.NoError(t, os.WriteFile("/tmp/query.sql", []byte("SELECT * FROM table;"), 0644))
		// act
		err := client.Execute(loader, "project_test.table_test", "/tmp/query.sql")
		// assert
		assert.Error(t, err)
		assert.ErrorContains(t, err, "error exec sql")
	})
	t.Run("should return nil when everything is successful", func(t *testing.T) {
		// arrange
		client := client.NewClient(slog.Default(), nil)
		client.OdpsClient = &mockOdpsClient{
			partitionResult: func() ([]string, error) {
				return []string{"event_date"}, nil
			},
			execSQLResult: func() error {
				return nil
			},
		}
		loader := &mockLoader{
			getQueryResult: func() string {
				return "INSERT INTO table SELECT * FROM table;"
			},
			getPartitionedQueryResult: func() string {
				return "INSERT INTO table PARTITION (event_date) SELECT * FROM table;"
			},
		}
		assert.NoError(t, os.WriteFile("/tmp/query.sql", []byte("SELECT * FROM table;"), 0644))
		// act
		err := client.Execute(loader, "project_test.table_test", "/tmp/query.sql")
		// assert
		assert.NoError(t, err)
	})
}

type mockOdpsClient struct {
	partitionResult func() ([]string, error)
	execSQLResult   func() error
}

func (m *mockOdpsClient) GetPartitionNames(tableID string) ([]string, error) {
	return m.partitionResult()
}

func (m *mockOdpsClient) ExecSQL(query string) error {
	return m.execSQLResult()
}

type mockLoader struct {
	getQueryResult            func() string
	getPartitionedQueryResult func() string
}

func (m *mockLoader) GetQuery(tableID, query string) string {
	return m.getQueryResult()
}

func (m *mockLoader) GetPartitionedQuery(tableID, query string, partitionName []string) string {
	return m.getPartitionedQueryResult()
}
