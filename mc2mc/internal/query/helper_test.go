package query_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/transformers/mc2mc/internal/query"
)

func TestSeparateHeadersAndQuery(t *testing.T) {
	t.Run("returns query without macros", func(t *testing.T) {
		q1 := `select * from playground`
		macros, query := query.SeparateHeadersAndQuery(q1)
		assert.Empty(t, macros)
		assert.Equal(t, q1, query)
	})
	t.Run("returns query removing whitespace", func(t *testing.T) {
		q1 := `
select * from playground`

		header, query := query.SeparateHeadersAndQuery(q1)
		assert.Empty(t, header)
		assert.Equal(t, "select * from playground", query)
	})
	t.Run("splits headers and query", func(t *testing.T) {
		q1 := `set odps.sql.allow.fullscan=true;
select * from playground`
		headers, query := query.SeparateHeadersAndQuery(q1)
		assert.Equal(t, "set odps.sql.allow.fullscan=true\n;", headers)
		assert.Equal(t, "select * from playground", query)
	})
	t.Run("splits headers and query with set syntax", func(t *testing.T) {
		q1 := `set odps.sql.allow.fullscan=true;
MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2;`
		headers, query := query.SeparateHeadersAndQuery(q1)
		assert.Equal(t, "set odps.sql.allow.fullscan=true\n;", headers)
		assert.Equal(t, `MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2`, query)
	})
	t.Run("works with query of multiple headers", func(t *testing.T) {
		q1 := `set odps.sql.allow.fullscan=true;
set odps.sql.python.version=cp37;

select distinct event_timestamp,
                client_id,
                country_code,
from presentation.main.important_date
where CAST(event_timestamp as DATE) = '{{ .DSTART | Date }}'
  and client_id in ('123')
`
		headers, query := query.SeparateHeadersAndQuery(q1)
		expectedHeader := `set odps.sql.allow.fullscan=true
;
set odps.sql.python.version=cp37
;`
		assert.Equal(t, expectedHeader, headers)

		expectedQuery := `select distinct event_timestamp,
                client_id,
                country_code,
from presentation.main.important_date
where CAST(event_timestamp as DATE) = '{{ .DSTART | Date }}'
  and client_id in ('123')`
		assert.Contains(t, expectedQuery, query)
	})
	t.Run("works with query contains semicolon", func(t *testing.T) {
		q1 := `set odps.sql.allow.fullscan=true;
select CONCAT_WS('; ', COLLECT_LIST(dates)) AS dates from presentation.main.important_date`
		headers, query := query.SeparateHeadersAndQuery(q1)
		expectedHeader := "set odps.sql.allow.fullscan=true\n;"
		assert.Equal(t, expectedHeader, headers)

		expectedQuery := `select CONCAT_WS('; ', COLLECT_LIST(dates)) AS dates from presentation.main.important_date`
		assert.Equal(t, expectedQuery, query)
	})
	t.Run("works with query with comment on header", func(t *testing.T) {
		q1 := `set odps.sql.allow.fullscan=true;
-- comment here
set odps.sql.python.version=cp37;

select distinct event_timestamp,
                client_id,
                country_code,
from presentation.main.important_date
where CAST(event_timestamp as DATE) = '{{ .DSTART | Date }}'
  and client_id in ('123')
`
		headers, query := query.SeparateHeadersAndQuery(q1)
		expectedHeader := `set odps.sql.allow.fullscan=true
;
-- comment here
set odps.sql.python.version=cp37
;`
		assert.Equal(t, expectedHeader, headers)

		expectedQuery := `select distinct event_timestamp,
                client_id,
                country_code,
from presentation.main.important_date
where CAST(event_timestamp as DATE) = '{{ .DSTART | Date }}'
  and client_id in ('123')`
		assert.Contains(t, expectedQuery, query)
	})
}

func TestSeparateVariablesUDFsAndQuery(t *testing.T) {
	t.Run("returns query without variables", func(t *testing.T) {
		q1 := `MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2;`
		variables, query := query.SeparateVariablesUDFsAndQuery(q1)
		assert.Empty(t, variables)
		assert.Equal(t, `MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2`, query)
	})
	t.Run("returns query removing whitespace", func(t *testing.T) {
		q1 := `
MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2;`

		variables, query := query.SeparateVariablesUDFsAndQuery(q1)
		assert.Empty(t, variables)
		assert.Equal(t, `MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2`, query)
	})
	t.Run("splits variables and query", func(t *testing.T) {
		q1 := `@src := SELECT 1 id;
MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2;`
		variables, query := query.SeparateVariablesUDFsAndQuery(q1)
		assert.Equal(t, "@src := SELECT 1 id\n;", variables)
		assert.Equal(t, `MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2`, query)
	})
	t.Run("splits multiline variables and queries", func(t *testing.T) {
		q1 := `@src := SELECT id
FROM src_table
WHERE id = 1;
@src2 := SELECT id
FROM src_table
WHERE id = 2;
MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2;
MERGE INTO append_test
USING (SELECT * FROM @src2) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 3;`
		variables, query := query.SeparateVariablesUDFsAndQuery(q1)
		assert.Equal(t, `@src := SELECT id
FROM src_table
WHERE id = 1
;
@src2 := SELECT id
FROM src_table
WHERE id = 2
;`, variables)
		assert.Equal(t, `MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2
;
MERGE INTO append_test
USING (SELECT * FROM @src2) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 3`, query)
	})
	t.Run("splits multiline variables + udfs and queries", func(t *testing.T) {
		q1 := `@src := SELECT id
FROM src_table
WHERE id = 1;
function my_add(@a BIGINT) as @a + 1;
@src2 := SELECT id
FROM src_table
WHERE id = 2;
MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2;
MERGE INTO append_test
USING (SELECT * FROM @src2) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 3;`
		variables, query := query.SeparateVariablesUDFsAndQuery(q1)
		assert.Equal(t, `@src := SELECT id
FROM src_table
WHERE id = 1
;
function my_add(@a BIGINT) as @a + 1
;
@src2 := SELECT id
FROM src_table
WHERE id = 2
;`, variables)
		assert.Equal(t, `MERGE INTO append_test
USING (SELECT * FROM @src) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 2
;
MERGE INTO append_test
USING (SELECT * FROM @src2) source
on append_test.id = source.id
WHEN MATCHED THEN UPDATE
SET append_test.id = 3`, query)
	})
}

func TestRemoveComments(t *testing.T) {
	t.Run("returns query without single comments", func(t *testing.T) {
		q1 := `-- comment here
SELECT * FROM project.dataset.table; -- comment there`
		query := query.RemoveComments(q1)
		assert.Equal(t, `
SELECT * FROM project.dataset.table; `, query)

	})
	t.Run("returns query without multiline comments", func(t *testing.T) {
		q1 := `/* comment here
    another
*/
SELECT * FROM project.dataset.table; -- comment there

`
		query := query.RemoveComments(q1)
		assert.Equal(t, `
SELECT * FROM project.dataset.table; 

`, query)
	})
	t.Run("returns query without comment and no changing query structure", func(t *testing.T) {
		q1 := `SELECT * -- comment here
FROM project.dataset.table;`
		query := query.RemoveComments(q1)
		assert.Equal(t, `SELECT * 
FROM project.dataset.table;`, query)
	})
}

func TestProtectedStringLiteral(t *testing.T) {
	t.Run("returns query with protected string literals", func(t *testing.T) {
		q1 := `SELECT * FROM project.dataset.table WHERE name = 'john' AND age = 20;`
		placeholders, query := query.ProtectedStringLiteral(q1)
		assert.Equal(t, map[string]string{
			"__STRING_PLACEHOLDER_0__": "'john'",
		}, placeholders)
		assert.Equal(t, `SELECT * FROM project.dataset.table WHERE name = __STRING_PLACEHOLDER_0__ AND age = 20;`, query)
	})
	t.Run("returns query with protected string literals with multiple strings", func(t *testing.T) {
		q1 := `SELECT * FROM project.dataset.table WHERE name = 'john' AND age = 20 AND city = 'new york';`
		placeholders, query := query.ProtectedStringLiteral(q1)
		assert.Equal(t, map[string]string{
			"__STRING_PLACEHOLDER_0__": "'john'",
			"__STRING_PLACEHOLDER_1__": "'new york'",
		}, placeholders)
		assert.Equal(t, `SELECT * FROM project.dataset.table WHERE name = __STRING_PLACEHOLDER_0__ AND age = 20 AND city = __STRING_PLACEHOLDER_1__;`, query)
	})
	t.Run("returns query with protected string literals with multiple strings and special characters", func(t *testing.T) {
		q1 := `SELECT * FROM project.dataset.table WHERE name = 'john' AND age = 20 AND city = 'new york' AND address = '1234 5th --Ave';`
		placeholders, query := query.ProtectedStringLiteral(q1)
		assert.Equal(t, map[string]string{
			"__STRING_PLACEHOLDER_0__": "'john'",
			"__STRING_PLACEHOLDER_1__": "'new york'",
			"__STRING_PLACEHOLDER_2__": "'1234 5th --Ave'",
		}, placeholders)
		assert.Equal(t, `SELECT * FROM project.dataset.table WHERE name = __STRING_PLACEHOLDER_0__ AND age = 20 AND city = __STRING_PLACEHOLDER_1__ AND address = __STRING_PLACEHOLDER_2__;`, query)
	})
}

func TestRestoreStringLiteral(t *testing.T) {
	t.Run("returns query with restored string literals", func(t *testing.T) {
		q1 := `SELECT * FROM project.dataset.table WHERE name = __STRING_PLACEHOLDER_0__ AND age = 20;`
		placeholders := map[string]string{
			"__STRING_PLACEHOLDER_0__": "'john'",
		}
		query := query.RestoreStringLiteral(q1, placeholders)
		assert.Equal(t, `SELECT * FROM project.dataset.table WHERE name = 'john' AND age = 20;`, query)
	})
	t.Run("returns query with restored string literals with multiple strings", func(t *testing.T) {
		q1 := `SELECT * FROM project.dataset.table WHERE name = __STRING_PLACEHOLDER_0__ AND age = 20 AND city = __STRING_PLACEHOLDER_1__;`
		placeholders := map[string]string{
			"__STRING_PLACEHOLDER_0__": "'john'",
			"__STRING_PLACEHOLDER_1__": "'new york'",
		}
		query := query.RestoreStringLiteral(q1, placeholders)
		assert.Equal(t, `SELECT * FROM project.dataset.table WHERE name = 'john' AND age = 20 AND city = 'new york';`, query)
	})
	t.Run("returns query with restored string literals with multiple strings and special characters", func(t *testing.T) {
		q1 := `SELECT * FROM project.dataset.table WHERE name = __STRING_PLACEHOLDER_0__ AND age = 20 AND city = __STRING_PLACEHOLDER_1__ AND address = __STRING_PLACEHOLDER_2__;`
		placeholders := map[string]string{
			"__STRING_PLACEHOLDER_0__": "'john'",
			"__STRING_PLACEHOLDER_1__": "'new york'",
			"__STRING_PLACEHOLDER_2__": "'1234 5th --Ave'",
		}
		query := query.RestoreStringLiteral(q1, placeholders)
		assert.Equal(t, `SELECT * FROM project.dataset.table WHERE name = 'john' AND age = 20 AND city = 'new york' AND address = '1234 5th --Ave';`, query)
	})
}
