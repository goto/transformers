package query_test

import (
	"context"
	"testing"

	"github.com/goto/transformers/mc2mc/internal/logger"
	"github.com/goto/transformers/mc2mc/internal/query"
	"github.com/stretchr/testify/assert"
)

func TestBuilder_Build(t *testing.T) {
	t.Run("returns query for merge load method", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;`
		odspClient := &mockOdpsClient{}

		query, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			queryToExecute,
			query.WithMethod(query.MERGE),
		).Build()
		assert.NoError(t, err)
		assert.Equal(t, queryToExecute, query)
	})
	t.Run("returns error for append load method when destination table is not specify", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;`
		odspClient := &mockOdpsClient{}

		queryToExecute, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			queryToExecute,
			query.WithMethod(query.APPEND),
		).Build()

		assert.Error(t, err)
		assert.Empty(t, queryToExecute)
	})
	t.Run("returns error for append load method when fail to get destination columns", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;`
		odspClient := &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return nil, assert.AnError
			},
		}
		destinationTableID := "project.playground.table_destination"

		queryToExecute, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			queryToExecute,
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithColumnOrder(),
		).Build()

		assert.Error(t, err)
		assert.Empty(t, queryToExecute)
	})
	t.Run("returns error for append load method when contains overrided values but no column order", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;`
		odspClient := &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return []string{"col1", "col2", "_partitiontime"}, nil
			},
			partitionResult: func() ([]string, error) {
				return []string{}, nil
			},
		}
		destinationTableID := "project.playground.table_destination"

		queryToExecute, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			queryToExecute,
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
		).Build()

		assert.Error(t, err)
		assert.Empty(t, queryToExecute)
	})
	t.Run("returns query for append load method when contains overrided values", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;`
		odspClient := &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return []string{"col1", "col2", "_partitiontime"}, nil
			},
			partitionResult: func() ([]string, error) {
				return []string{}, nil
			},
		}
		destinationTableID := "project.playground.table_destination"

		queryToExecute, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			queryToExecute,
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (SELECT col1, col2, _partitiontime FROM (select * from project.playground.table));`, queryToExecute)
	})
	t.Run("returns query for append load method when temporary partition_value enable", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;`
		odspClient := &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return []string{"col1", "col2", "_partitiontime"}, nil
			},
			partitionResult: func() ([]string, error) {
				return []string{}, nil
			},
		}
		destinationTableID := "project.playground.table_destination"

		queryToExecute, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			queryToExecute,
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithPartitionValue(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination SELECT *, STRING(CURRENT_DATE()) as __partitionvalue FROM (SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (SELECT col1, col2, _partitiontime FROM (select * from project.playground.table)));`, queryToExecute)
	})
	t.Run("returns query for append load method when auto partition enable", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;`
		odspClient := &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return []string{"col1", "col2", "_partitiontime"}, nil
			},
			partitionResult: func() ([]string, error) {
				return []string{}, nil
			},
		}
		destinationTableID := "project.playground.table_destination"

		queryToExecute, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			queryToExecute,
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithAutoPartition(true),
			query.WithPartitionValue(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (SELECT col1, col2, _partitiontime FROM (select * from project.playground.table));`, queryToExecute)
	})
	t.Run("returns query for append load method for partition table", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;`
		odspClient := &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return []string{"col1", "col2", "_partitiontime"}, nil
			},
			partitionResult: func() ([]string, error) {
				return []string{"col3"}, nil
			},
		}
		destinationTableID := "project.playground.table_destination"

		queryToExecute, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			queryToExecute,
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination PARTITION (col3) SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (SELECT col1, col2, _partitiontime FROM (select * from project.playground.table));`, queryToExecute)
	})
	t.Run("returns query for append load method for partition table but autopartition enable", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;`
		odspClient := &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return []string{"col1", "col2", "_partitiontime"}, nil
			},
			partitionResult: func() ([]string, error) {
				return []string{"col3"}, nil
			},
		}
		destinationTableID := "project.playground.table_destination"

		queryToExecute, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			queryToExecute,
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithAutoPartition(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (SELECT col1, col2, _partitiontime FROM (select * from project.playground.table));`, queryToExecute)
	})
	t.Run("returns query for append load method", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;`
		odspClient := &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return []string{"col1", "col2", "_partitiontime"}, nil
			},
			partitionResult: func() ([]string, error) {
				return []string{"col3"}, nil
			},
		}
		destinationTableID := "project.playground.table_destination"

		queryToExecute, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			queryToExecute,
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithOverridedValue("_partitiondate", "DATE(TIMESTAMP('2021-01-01'))"),
			query.WithAutoPartition(true),
			query.WithPartitionValue(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (SELECT col1, col2, _partitiontime FROM (select * from project.playground.table));`, queryToExecute)
	})
	t.Run("returns query for replace load method", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;`
		odspClient := &mockOdpsClient{
			orderedColumns: func() ([]string, error) {
				return []string{"col1", "col2", "_partitiontime"}, nil
			},
			partitionResult: func() ([]string, error) {
				return []string{"col3"}, nil
			},
		}
		destinationTableID := "project.playground.table_destination"

		queryToExecute, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			queryToExecute,
			query.WithMethod(query.REPLACE),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithOverridedValue("_partitiondate", "DATE(TIMESTAMP('2021-01-01'))"),
			query.WithAutoPartition(true),
			query.WithPartitionValue(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT OVERWRITE TABLE project.playground.table_destination SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (SELECT col1, col2, _partitiontime FROM (select * from project.playground.table));`, queryToExecute)
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
