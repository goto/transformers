package upstream

import (
	"context"
	"errors"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
)

type Extractor struct {
	client bqiface.Client

	schemaToUpstreams map[string][]*Upstream
}

func NewExtractor(client bqiface.Client) (*Extractor, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}

	return &Extractor{
		client:            client,
		schemaToUpstreams: make(map[string][]*Upstream),
	}, nil
}

func (e *Extractor) ExtractUpstreams(ctx context.Context, query string, resourcesToIgnore []Resource) ([]*Upstream, error) {
	ignoredResources := make(map[Resource]bool)
	for _, r := range resourcesToIgnore {
		ignoredResources[r] = true
	}

	metResource := make(map[Resource]bool)
	return e.extractUpstreamsFromQuery(ctx, query, ignoredResources, metResource, ParseTopLevelUpstreamsFromQuery)
}

func (e *Extractor) extractUpstreamsFromQuery(
	ctx context.Context, query string,
	ignoredResources, metResource map[Resource]bool,
	parseFn QueryParser,
) ([]*Upstream, error) {
	upstreamResources := parseFn(query)

	uniqueUpstreamResources := UniqueFilterResources(upstreamResources)

	filteredUpstreamResources := FilterResources(uniqueUpstreamResources, func(r Resource) bool { return ignoredResources[r] })

	resourceGroups := GroupResources(filteredUpstreamResources)

	var output []*Upstream
	for _, group := range resourceGroups {
		schemas, err := ReadSchemasUnderGroup(ctx, e.client, group)
		if err != nil {
			return nil, err
		}

		nestedable, rest := splitNestedableFromRest(schemas)

		restsNodes := convertSchemasToNodes(rest)
		output = append(output, restsNodes...)

		nestedNodes, err := e.extractNestedNodes(ctx, nestedable, ignoredResources, metResource)
		if err != nil {
			return nil, err
		}

		output = append(output, nestedNodes...)
	}

	return output, nil
}

func (e *Extractor) extractNestedNodes(
	ctx context.Context, schemas []*Schema,
	ignoredResources, metResource map[Resource]bool,
) ([]*Upstream, error) {
	output := make([]*Upstream, len(schemas))

	for i, sch := range schemas {
		if metResource[sch.Resource] {
			continue
		}

		nodes, err := e.getNodes(ctx, sch, ignoredResources, metResource)
		if err != nil {
			return nil, err
		}

		metResource[sch.Resource] = true
		output[i] = &Upstream{
			Resource:  sch.Resource,
			Upstreams: nodes,
		}
	}

	return output, nil
}

func (e *Extractor) getNodes(
	ctx context.Context, schema *Schema,
	ignoredResources, metResource map[Resource]bool,
) ([]*Upstream, error) {
	key := schema.Resource.URN()

	if existingNodes, ok := e.schemaToUpstreams[key]; ok {
		return existingNodes, nil
	}

	nodes, err := e.extractUpstreamsFromQuery(ctx, schema.DDL, ignoredResources, metResource, ParseNestedUpsreamsFromDDL)
	if err != nil {
		return nil, err
	}

	e.schemaToUpstreams[key] = nodes

	return nodes, nil
}
