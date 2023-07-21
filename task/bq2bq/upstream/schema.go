package upstream

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"google.golang.org/api/iterator"
)

const wildCardSuffix = "*"

type SchemaType string

const (
	Unknown SchemaType = "UNKNOWN"

	BaseTable        SchemaType = "BASE TABLE"
	External         SchemaType = "EXTERNAL"
	MaterializedView SchemaType = "MATERIALIZED VIEW"
	Snapshot         SchemaType = "SNAPSHOT"
	View             SchemaType = "VIEW"
)

type Schema struct {
	Resource Resource
	Type     SchemaType
	DDL      string
}

func ReadSchemasUnderGroup(ctx context.Context, client bqiface.Client, group *ResourceGroup) ([]*Schema, error) {
	queryContent := buildQuery(group)

	queryStatement := client.Query(queryContent)

	rowIterator, err := queryStatement.Read(ctx)
	if err != nil {
		return nil, err
	}

	var schemas []*Schema
	for {
		var values []bigquery.Value
		err := rowIterator.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(values) == 0 {
			continue
		}

		sch, err := convertToSchema(values)
		if err != nil {
			return nil, err
		}

		schemas = append(schemas, sch)
	}

	return schemas, nil
}

func buildQuery(group *ResourceGroup) string {
	var nameQueries, prefixQueries []string
	for _, n := range group.Names {
		if strings.HasSuffix(n, wildCardSuffix) {
			prefix, _ := strings.CutSuffix(n, wildCardSuffix)
			prefixQuery := fmt.Sprintf("STARTS_WITH(table_name, '%s')", prefix)
			prefixQueries = append(prefixQueries, prefixQuery)
		} else {
			nameQuery := fmt.Sprintf("'%s'", n)
			nameQueries = append(nameQueries, nameQuery)
		}
	}

	names := strings.Join(nameQueries, ", ")
	prefixes := strings.Join(prefixQueries, " or\n")

	var whereClause string
	if len(nameQueries) > 0 && len(prefixQueries) > 0 {
		whereClause = fmt.Sprintf("WHERE table_name in (%s) or %s", names, prefixes)
	} else if len(nameQueries) > 0 {
		whereClause = fmt.Sprintf("WHERE table_name in (%s)", names)
	} else if len(prefixQueries) > 0 {
		whereClause = fmt.Sprintf("WHERE %s", prefixes)
	}

	return "SELECT table_catalog, table_schema, table_name, table_type, ddl\n" +
		fmt.Sprintf("FROM `%s.%s.INFORMATION_SCHEMA.TABLES`\n", group.Project, group.Dataset) +
		whereClause
}

func convertToSchema(values []bigquery.Value) (*Schema, error) {
	const expectedSchemaRowLen = 5

	if l := len(values); l != expectedSchemaRowLen {
		return nil, fmt.Errorf("unexpected number of row length: %d", l)
	}

	project, ok := values[0].(string)
	if !ok {
		return nil, errors.New("error casting project")
	}

	dataset, ok := values[1].(string)
	if !ok {
		return nil, errors.New("error casting dataset")
	}

	name, ok := values[2].(string)
	if !ok {
		return nil, errors.New("error casting name")
	}

	_type, ok := values[3].(string)
	if !ok {
		return nil, errors.New("error casting _type")
	}

	ddl, ok := values[4].(string)
	if !ok {
		return nil, errors.New("error casting ddl")
	}

	resource := Resource{
		Project: project,
		Dataset: dataset,
		Name:    name,
	}

	var schemaType SchemaType
	switch _type {
	case string(BaseTable):
		schemaType = BaseTable
	case string(External):
		schemaType = External
	case string(MaterializedView):
		schemaType = MaterializedView
	case string(Snapshot):
		schemaType = Snapshot
	case string(View):
		schemaType = View
	default:
		schemaType = Unknown
	}

	return &Schema{
		Resource: resource,
		Type:     schemaType,
		DDL:      ddl,
	}, nil
}

func splitNestedableFromRest(schemas []*Schema) (nestedable, rests []*Schema) {
	for _, sch := range schemas {
		switch sch.Type {
		case View:
			nestedable = append(nestedable, sch)
		default:
			rests = append(rests, sch)
		}
	}

	return nestedable, rests
}

func convertSchemasToNodes(schemas []*Schema) []*Upstream {
	output := make([]*Upstream, len(schemas))
	for i, sch := range schemas {
		output[i] = &Upstream{
			Resource: sch.Resource,
		}
	}

	return output
}
