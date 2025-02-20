package query_test

import (
	"context"
	"testing"

	"github.com/goto/transformers/mc2mc/internal/logger"
	"github.com/goto/transformers/mc2mc/internal/query"
	"github.com/stretchr/testify/assert"
)

func TestBuilder_Build(t *testing.T) {
	t.Run("returns error for empty query", func(t *testing.T) {
		odspClient := &mockOdpsClient{}

		queryToExecute, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
		).Build()
		assert.Error(t, err)
		assert.Empty(t, queryToExecute)
	})
	t.Run("returns query for merge load method", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;`
		odspClient := &mockOdpsClient{}

		query, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			query.WithQuery(queryToExecute),
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
			query.WithQuery(queryToExecute),
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
			query.WithQuery(queryToExecute),
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithColumnOrder(),
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
			query.WithQuery(queryToExecute),
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination 
SELECT col1, col2, _partitiontime FROM (
SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (
select * from project.playground.table
)
)
;`, queryToExecute)
	})
	t.Run("returns query for append load method when contains overrided values but no column order", func(t *testing.T) {
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
			query.WithQuery(queryToExecute),
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination 
SELECT col1, col2, _partitiontime FROM (
SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (
select * from project.playground.table
)
)
;`, queryToExecute)
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
			query.WithQuery(queryToExecute),
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithPartitionValue(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination 
SELECT *, STRING(CURRENT_DATE()) as __partitionvalue FROM (
SELECT col1, col2, _partitiontime FROM (
SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (
select * from project.playground.table
)
)
)
;`, queryToExecute)
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
			query.WithQuery(queryToExecute),
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithAutoPartition(true),
			query.WithPartitionValue(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination 
SELECT col1, col2, _partitiontime FROM (
SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (
select * from project.playground.table
)
)
;`, queryToExecute)
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
			query.WithQuery(queryToExecute),
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination PARTITION (col3) 
SELECT col1, col2, _partitiontime FROM (
SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (
select * from project.playground.table
)
)
;`, queryToExecute)
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
			query.WithQuery(queryToExecute),
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithAutoPartition(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination 
SELECT col1, col2, _partitiontime FROM (
SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (
select * from project.playground.table
)
)
;`, queryToExecute)
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
			query.WithQuery(queryToExecute),
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithOverridedValue("_partitiondate", "DATE(TIMESTAMP('2021-01-01'))"),
			query.WithAutoPartition(true),
			query.WithPartitionValue(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT INTO TABLE project.playground.table_destination 
SELECT col1, col2, _partitiontime FROM (
SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (
select * from project.playground.table
)
)
;`, queryToExecute)
	})
	t.Run("returns query for append load method when contains hrs, vars and udfs", func(t *testing.T) {
		queryToExecute := `set odps.table.append2.enable=true;
set odps.table.append3.enable=true; 	
-- this is comment
function my_add(@a BIGINT) as @a + 1;
/* maybe
another comment */
@src := SELECT my_add(1) id;
select * from project.playground.table;`
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
			query.WithQuery(queryToExecute),
			query.WithMethod(query.APPEND),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithOverridedValue("_partitiondate", "DATE(TIMESTAMP('2021-01-01'))"),
			query.WithAutoPartition(true),
			query.WithPartitionValue(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `set odps.table.append2.enable=true
;
set odps.table.append3.enable=true
;
-- this is comment
function my_add(@a BIGINT) as @a + 1
;
/* maybe
another comment */
@src := SELECT my_add(1) id
;
INSERT INTO TABLE project.playground.table_destination 
SELECT col1, col2, _partitiontime FROM (
SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (
select * from project.playground.table
)
)
;`, queryToExecute)
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
			query.WithQuery(queryToExecute),
			query.WithMethod(query.REPLACE),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithOverridedValue("_partitiondate", "DATE(TIMESTAMP('2021-01-01'))"),
			query.WithAutoPartition(true),
			query.WithPartitionValue(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT OVERWRITE TABLE project.playground.table_destination 
SELECT col1, col2, _partitiontime FROM (
SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (
select * from project.playground.table
)
)
;`, queryToExecute)
	})
	t.Run("returns query for replace load method with comment in the end", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table
-- this is comment`
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
			query.WithQuery(queryToExecute),
			query.WithMethod(query.REPLACE),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithOverridedValue("_partitiondate", "DATE(TIMESTAMP('2021-01-01'))"),
			query.WithAutoPartition(true),
			query.WithPartitionValue(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT OVERWRITE TABLE project.playground.table_destination 
SELECT col1, col2, _partitiontime FROM (
SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (
select * from project.playground.table
-- this is comment
)
)
;`, queryToExecute)
	})
	t.Run("returns query for replace load method with comment in the end with semicolon", func(t *testing.T) {
		queryToExecute := `select * from project.playground.table;
		-- this is comment;`
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
			query.WithQuery(queryToExecute),
			query.WithMethod(query.REPLACE),
			query.WithDestination(destinationTableID),
			query.WithOverridedValue("_partitiontime", "TIMESTAMP('2021-01-01')"),
			query.WithOverridedValue("_partitiondate", "DATE(TIMESTAMP('2021-01-01'))"),
			query.WithAutoPartition(true),
			query.WithPartitionValue(true),
			query.WithColumnOrder(),
		).Build()

		assert.NoError(t, err)
		assert.Equal(t, `INSERT OVERWRITE TABLE project.playground.table_destination 
SELECT col1, col2, _partitiontime FROM (
SELECT col1, col2, TIMESTAMP('2021-01-01') as _partitiontime FROM (
select * from project.playground.table
)
)
;`, queryToExecute)
	})
	t.Run("returns query for merge load method with single dml", func(t *testing.T) {
		queryToExecute := `SET odps.table.append2.enable=true;
@src := SELECT 1 id;

MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2;`
		odspClient := &mockOdpsClient{}

		query, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			query.WithQuery(queryToExecute),
			query.WithMethod(query.MERGE),
		).Build()
		assert.NoError(t, err)
		assert.Equal(t, queryToExecute, query)
	})
	t.Run("returns query for merge load method with drop and create table", func(t *testing.T) {
		queryToExecute := `SET odps.table.append2.enable=true;
DROP TABLE IF EXISTS append_tmp;
@src := SELECT 1 id;

CREATE TABLE append_tmp AS SELECT * FROM @src;

MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2;`
		odspClient := &mockOdpsClient{}

		query, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			query.WithQuery(queryToExecute),
			query.WithMethod(query.MERGE),
		).Build()
		assert.NoError(t, err)
		assert.Equal(t, `SET odps.table.append2.enable=true
;
DROP TABLE IF EXISTS append_tmp
;
--*--optimus-break-marker--*--
SET odps.table.append2.enable=true
;
@src := SELECT 1 id
;
CREATE TABLE append_tmp AS SELECT * FROM @src
;
--*--optimus-break-marker--*--
SET odps.table.append2.enable=true
;
@src := SELECT 1 id
;
MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2
;`, query)
	})
	t.Run("returns query for merge load method with multiple dml and ddl", func(t *testing.T) {
		queryToExecute := `SET odps.table.append2.enable=true;

CREATE TABLE IF NOT EXISTS append_test (id bigint)
TBLPROPERTIES('table.format.version'='2');

INSERT OVERWRITE TABLE append_test VALUES(0),(1);

@src := SELECT 1 id;

MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2;`
		odspClient := &mockOdpsClient{}

		query, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			query.WithQuery(queryToExecute),
			query.WithMethod(query.MERGE),
		).Build()
		assert.NoError(t, err)
		assert.Equal(t, `SET odps.table.append2.enable=true
;
@src := SELECT 1 id
;
CREATE TABLE IF NOT EXISTS append_test (id bigint)
TBLPROPERTIES('table.format.version'='2')
;
--*--optimus-break-marker--*--
SET odps.table.append2.enable=true
;
@src := SELECT 1 id
;
INSERT OVERWRITE TABLE append_test VALUES(0),(1)
;
--*--optimus-break-marker--*--
SET odps.table.append2.enable=true
;
@src := SELECT 1 id
;
MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2
;`, query)
	})
	t.Run("returns query for merge load method with multiple dml and ddl and contains function", func(t *testing.T) {
		queryToExecute := `SET odps.table.append2.enable=true;

CREATE TABLE IF NOT EXISTS append_test (id bigint)
TBLPROPERTIES('table.format.version'='2');

FUNCTION castStringToBoolean (@field STRING) AS CASE
WHEN TOLOWER(@field) = '1.0' THEN true
WHEN TOLOWER(@field) = '0.0' THEN false
WHEN TOLOWER(@field) = '1' THEN true
WHEN TOLOWER(@field) = '0' THEN false
WHEN TOLOWER(@field) = 'true' THEN true
WHEN TOLOWER(@field) = 'false' THEN false
END;

function my_add(@a BIGINT) as @a + 1;

INSERT OVERWRITE TABLE append_test VALUES(0),(1);

@src := SELECT my_add(1) id;

MERGE INTO append_test
USING (SELECT castStringToBoolean(id) FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2;`
		odspClient := &mockOdpsClient{}

		query, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			query.WithQuery(queryToExecute),
			query.WithMethod(query.MERGE),
		).Build()
		assert.NoError(t, err)
		assert.Equal(t, `SET odps.table.append2.enable=true
;
FUNCTION castStringToBoolean (@field STRING) AS CASE
WHEN TOLOWER(@field) = '1.0' THEN true
WHEN TOLOWER(@field) = '0.0' THEN false
WHEN TOLOWER(@field) = '1' THEN true
WHEN TOLOWER(@field) = '0' THEN false
WHEN TOLOWER(@field) = 'true' THEN true
WHEN TOLOWER(@field) = 'false' THEN false
END
;
function my_add(@a BIGINT) as @a + 1
;
@src := SELECT my_add(1) id
;
CREATE TABLE IF NOT EXISTS append_test (id bigint)
TBLPROPERTIES('table.format.version'='2')
;
--*--optimus-break-marker--*--
SET odps.table.append2.enable=true
;
FUNCTION castStringToBoolean (@field STRING) AS CASE
WHEN TOLOWER(@field) = '1.0' THEN true
WHEN TOLOWER(@field) = '0.0' THEN false
WHEN TOLOWER(@field) = '1' THEN true
WHEN TOLOWER(@field) = '0' THEN false
WHEN TOLOWER(@field) = 'true' THEN true
WHEN TOLOWER(@field) = 'false' THEN false
END
;
function my_add(@a BIGINT) as @a + 1
;
@src := SELECT my_add(1) id
;
INSERT OVERWRITE TABLE append_test VALUES(0),(1)
;
--*--optimus-break-marker--*--
SET odps.table.append2.enable=true
;
FUNCTION castStringToBoolean (@field STRING) AS CASE
WHEN TOLOWER(@field) = '1.0' THEN true
WHEN TOLOWER(@field) = '0.0' THEN false
WHEN TOLOWER(@field) = '1' THEN true
WHEN TOLOWER(@field) = '0' THEN false
WHEN TOLOWER(@field) = 'true' THEN true
WHEN TOLOWER(@field) = 'false' THEN false
END
;
function my_add(@a BIGINT) as @a + 1
;
@src := SELECT my_add(1) id
;
MERGE INTO append_test
USING (SELECT castStringToBoolean(id) FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2
;`, query)
	})
	t.Run("returns query for merge load method when there's a comment with semicolon", func(t *testing.T) {
		queryToExecute := `SET odps.table.append2.enable=true;

CREATE TABLE IF NOT EXISTS append_test (id bigint)
TBLPROPERTIES('table.format.version'='2');

FUNCTION castStringToBoolean (@field STRING) AS CASE
WHEN TOLOWER(@field) = '1.0' THEN true
WHEN TOLOWER(@field) = '0.0' THEN false
WHEN TOLOWER(@field) = '1' THEN true
WHEN TOLOWER(@field) = '0' THEN false
WHEN TOLOWER(@field) = 'true' THEN true
WHEN TOLOWER(@field) = 'false' THEN false
END;

function my_add(@a BIGINT) as @a + 1;

INSERT OVERWRITE TABLE append_test VALUES(0),(1);

@src := SELECT my_add(1) id;

-- this is comment;

MERGE INTO append_test
USING (SELECT castStringToBoolean(id) FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2;`
		odspClient := &mockOdpsClient{}

		query, err := query.NewBuilder(
			logger.NewDefaultLogger(),
			odspClient,
			query.WithQuery(queryToExecute),
			query.WithMethod(query.MERGE),
		).Build()
		assert.NoError(t, err)
		assert.Equal(t, `SET odps.table.append2.enable=true
;
FUNCTION castStringToBoolean (@field STRING) AS CASE
WHEN TOLOWER(@field) = '1.0' THEN true
WHEN TOLOWER(@field) = '0.0' THEN false
WHEN TOLOWER(@field) = '1' THEN true
WHEN TOLOWER(@field) = '0' THEN false
WHEN TOLOWER(@field) = 'true' THEN true
WHEN TOLOWER(@field) = 'false' THEN false
END
;
function my_add(@a BIGINT) as @a + 1
;
@src := SELECT my_add(1) id
;
CREATE TABLE IF NOT EXISTS append_test (id bigint)
TBLPROPERTIES('table.format.version'='2')
;
--*--optimus-break-marker--*--
SET odps.table.append2.enable=true
;
FUNCTION castStringToBoolean (@field STRING) AS CASE
WHEN TOLOWER(@field) = '1.0' THEN true
WHEN TOLOWER(@field) = '0.0' THEN false
WHEN TOLOWER(@field) = '1' THEN true
WHEN TOLOWER(@field) = '0' THEN false
WHEN TOLOWER(@field) = 'true' THEN true
WHEN TOLOWER(@field) = 'false' THEN false
END
;
function my_add(@a BIGINT) as @a + 1
;
@src := SELECT my_add(1) id
;
INSERT OVERWRITE TABLE append_test VALUES(0),(1)
;
--*--optimus-break-marker--*--
SET odps.table.append2.enable=true
;
FUNCTION castStringToBoolean (@field STRING) AS CASE
WHEN TOLOWER(@field) = '1.0' THEN true
WHEN TOLOWER(@field) = '0.0' THEN false
WHEN TOLOWER(@field) = '1' THEN true
WHEN TOLOWER(@field) = '0' THEN false
WHEN TOLOWER(@field) = 'true' THEN true
WHEN TOLOWER(@field) = 'false' THEN false
END
;
function my_add(@a BIGINT) as @a + 1
;
@src := SELECT my_add(1) id
;
MERGE INTO append_test
USING (SELECT castStringToBoolean(id) FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2
;`, query)
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
