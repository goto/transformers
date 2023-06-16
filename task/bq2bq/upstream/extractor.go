package upstream

import (
	"context"
	"errors"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
)

type Extractor struct {
	client bqiface.Client

	ignoredResources map[string]bool

	schemaToUpstreams map[string][]*Upstream
}

func NewExtractor(client bqiface.Client, resourcesToIgnore []Resource) (*Extractor, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}

	ignoredResources := make(map[string]bool)
	for _, r := range resourcesToIgnore {
		key := r.URN()
		ignoredResources[key] = true
	}

	return &Extractor{
		client:            client,
		ignoredResources:  ignoredResources,
		schemaToUpstreams: make(map[string][]*Upstream),
	}, nil
}

func (e *Extractor) ExtractUpstreams(ctx context.Context, query string) ([]*Upstream, error) {
	return e.extractUpstreamsFromQuery(ctx, query, ParseTopLevelUpstreamsFromQuery)
}

func (e *Extractor) extractUpstreamsFromQuery(ctx context.Context, query string, parseFn QueryParser) ([]*Upstream, error) {
	upstreamResources := parseFn(query)

	uniqueUpstreamResources := UniqueFilterResources(upstreamResources)

	filteredUpstreamResources := FilterResources(uniqueUpstreamResources, e.equalToIgnoredResources)

	resourceGroups := GroupResources(filteredUpstreamResources)

	output := make([]*Upstream, 0)
	for _, group := range resourceGroups {
		schemas, err := ReadSchemasUnderGroup(ctx, e.client, group)
		if err != nil {
			return nil, err
		}

		nestedable, rest := splitNestedableFromRest(schemas)

		restsNodes := convertSchemasToNodes(rest)
		output = append(output, restsNodes...)

		nestedNodes, err := e.extractNestedNodes(ctx, nestedable)
		if err != nil {
			return nil, err
		}

		output = append(output, nestedNodes...)
	}

	return output, nil
}

func (e *Extractor) extractNestedNodes(ctx context.Context, schemas []*Schema) ([]*Upstream, error) {
	output := make([]*Upstream, len(schemas))

	for i, sch := range schemas {
		nodes, err := e.getNodes(ctx, sch)
		if err != nil {
			return nil, err
		}

		output[i] = &Upstream{
			Resource:  sch.Resource,
			Upstreams: nodes,
		}
	}

	return output, nil
}

func (e *Extractor) equalToIgnoredResources(r Resource) bool {
	return e.ignoredResources[r.URN()]
}

func (e *Extractor) getNodes(ctx context.Context, schema *Schema) ([]*Upstream, error) {
	key := schema.Resource.URN()

	if existingNodes, ok := e.schemaToUpstreams[key]; ok {
		return existingNodes, nil
	}

	nodes, err := e.extractUpstreamsFromQuery(ctx, schema.DDL, ParseNestedUpsreamsFromDDL)
	if err != nil {
		return nil, err
	}

	e.schemaToUpstreams[key] = nodes

	return nodes, nil
}
