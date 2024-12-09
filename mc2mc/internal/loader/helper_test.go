package loader_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/transformers/mc2mc/internal/loader"
)

func TestMacroSeparator(t *testing.T) {
	t.Run("returns query without macros", func(t *testing.T) {
		q1 := `select * from playground`
		macros, query := loader.SeparateHeadersAndQuery(q1)
		assert.Empty(t, macros)
		assert.Equal(t, q1, query)
	})
	t.Run("returns query removing whitespace", func(t *testing.T) {
		q1 := `
select * from playground`

		header, query := loader.SeparateHeadersAndQuery(q1)
		assert.Empty(t, header)
		assert.Contains(t, query, q1)
	})
	t.Run("splits headers and query", func(t *testing.T) {
		q1 := `set odps.sql.allow.fullscan=true;
select * from playground`
		headers, query := loader.SeparateHeadersAndQuery(q1)
		assert.Equal(t, "set odps.sql.allow.fullscan=true;", headers)
		assert.Equal(t, "select * from playground", strings.TrimSpace(query))
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
		headers, query := loader.SeparateHeadersAndQuery(q1)
		expectedHeader := `set odps.sql.allow.fullscan=true;
set odps.sql.python.version=cp37;`
		assert.Equal(t, expectedHeader, headers)

		expectedQuery := `select distinct event_timestamp,
                client_id,
                country_code,
from presentation.main.important_date
where CAST(event_timestamp as DATE) = '{{ .DSTART | Date }}'
  and client_id in ('123')`
		assert.Contains(t, query, expectedQuery)
	})
}

func TestConstructQueryWithOrderedColumns(t *testing.T) {
	t.Run("returns query with ordered columns", func(t *testing.T) {
		q1 := `select col_2 as col2, col_3 as col3, col_1 as col1 from project.schema.table`
		orderedColumns := []string{"col1", "col2", "col3"}
		query := loader.ConstructQueryWithOrderedColumns(q1, orderedColumns)
		expected := "SELECT col1, col2, col3 FROM (select col_2 as col2, col_3 as col3, col_1 as col1 from project.schema.table)"
		assert.Equal(t, expected, query)
	})
}
