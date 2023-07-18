package upstream

import (
	"context"
	"errors"
	"sync"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
)

type Extractor struct {
	mutex  *sync.Mutex
	client bqiface.Client

	schemaToUpstreams map[string][]*Upstream
}

func NewExtractor(client bqiface.Client) (*Extractor, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}

	return &Extractor{
		mutex:             &sync.Mutex{},
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
	var output []*Upstream

	for _, sch := range schemas {
		if metResource[sch.Resource] {
			continue
		}
		metResource[sch.Resource] = true

		nodes, err := e.getNodes(ctx, sch, ignoredResources, metResource)
		if err != nil {
			return nil, err
		}

		output = append(output, &Upstream{
			Resource:  sch.Resource,
			Upstreams: nodes,
		})
	}

	return output, nil
}

func (e *Extractor) getNodes(
	ctx context.Context, schema *Schema,
	ignoredResources, metResource map[Resource]bool,
) ([]*Upstream, error) {
	key := schema.Resource.URN()

	e.mutex.Lock()
	existingNodes, ok := e.schemaToUpstreams[key]
	e.mutex.Unlock()

	if ok {
		return existingNodes, nil
	}

	nodes, err := e.extractUpstreamsFromQuery(ctx, schema.DDL, ignoredResources, metResource, ParseNestedUpsreamsFromDDL)
	if err != nil {
		return nil, err
	}

	e.mutex.Lock()
	e.schemaToUpstreams[key] = nodes
	e.mutex.Unlock()

	return nodes, nil
}
