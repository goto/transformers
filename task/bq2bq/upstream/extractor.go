package upstream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
)

type Extractor struct {
	mutex  *sync.Mutex
	client bqiface.Client

	urnToUpstreams map[string][]*Upstream
}

func NewExtractor(client bqiface.Client) (*Extractor, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}

	return &Extractor{
		mutex:          &sync.Mutex{},
		client:         client,
		urnToUpstreams: make(map[string][]*Upstream),
	}, nil
}

func (e *Extractor) ExtractUpstreams(ctx context.Context, query string, resourcesToIgnore []Resource) ([]*Upstream, error) {
	ignoredResources := make(map[Resource]bool)
	for _, r := range resourcesToIgnore {
		ignoredResources[r] = true
	}

	encounteredResources := make(map[Resource]bool)
	return e.extractUpstreamsFromQuery(ctx, query, ignoredResources, encounteredResources, ParseTopLevelUpstreamsFromQuery)
}

func (e *Extractor) extractUpstreamsFromQuery(
	ctx context.Context, query string,
	ignoredResources, encounteredResources map[Resource]bool,
	parseFn QueryParser,
) ([]*Upstream, error) {
	resources := parseFn(query)

	uniqueResources := UniqueFilterResources(resources)

	filteredResources := FilterResources(uniqueResources, func(r Resource) bool { return ignoredResources[r] })

	resourceGroups := GroupResources(filteredResources)

	var output []*Upstream
	var errorMessages []string

	for _, group := range resourceGroups {
		schemas, err := ReadSchemasUnderGroup(ctx, e.client, group)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}

		nestedable, unnestedable := splitNestedableFromRest(schemas)

		unnestedableUpstreams := convertSchemasToUpstreams(unnestedable)
		output = append(output, unnestedableUpstreams...)

		nestedUpstreams, err := e.extractNestedableUpstreams(ctx, nestedable, ignoredResources, encounteredResources)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}

		output = append(output, nestedUpstreams...)
	}

	if len(errorMessages) > 0 {
		return output, fmt.Errorf("error reading upstream: [%s]", strings.Join(errorMessages, ", "))
	}
	return output, nil
}

func (e *Extractor) extractNestedableUpstreams(
	ctx context.Context, schemas []*Schema,
	ignoredResources, encounteredResources map[Resource]bool,
) ([]*Upstream, error) {
	var output []*Upstream
	var errorMessages []string

	for _, sch := range schemas {
		if encounteredResources[sch.Resource] {
			msg := fmt.Sprintf("circular reference is detected: [%s]", e.getCircularURNs(encounteredResources))
			errorMessages = append(errorMessages, msg)
			continue
		}
		encounteredResources[sch.Resource] = true

		upstreams, err := e.getUpstreams(ctx, sch, ignoredResources, encounteredResources)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}

		output = append(output, &Upstream{
			Resource:  sch.Resource,
			Upstreams: upstreams,
		})
	}

	if len(errorMessages) > 0 {
		return output, fmt.Errorf("error getting nested upstream: [%s]", strings.Join(errorMessages, ", "))
	}
	return output, nil
}

func (e *Extractor) getUpstreams(
	ctx context.Context, schema *Schema,
	ignoredResources, encounteredResources map[Resource]bool,
) ([]*Upstream, error) {
	key := schema.Resource.URN()

	e.mutex.Lock()
	existingUpstreams, ok := e.urnToUpstreams[key]
	e.mutex.Unlock()

	if ok {
		return existingUpstreams, nil
	}

	upstreams, err := e.extractUpstreamsFromQuery(ctx, schema.DDL, ignoredResources, encounteredResources, ParseNestedUpsreamsFromDDL)

	e.mutex.Lock()
	e.urnToUpstreams[key] = upstreams
	e.mutex.Unlock()

	return upstreams, err
}

func (*Extractor) getCircularURNs(encounteredResources map[Resource]bool) string {
	var urns []string
	for resource := range encounteredResources {
		urns = append(urns, resource.URN())
	}

	return strings.Join(urns, ", ")
}
