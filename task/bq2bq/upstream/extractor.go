package upstream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/hashicorp/go-hclog"
)

type Extractor struct {
	mutex  *sync.Mutex
	client bqiface.Client

	urnToUpstreams map[string][]Resource
	logger         hclog.Logger
}

func NewExtractor(client bqiface.Client, logger hclog.Logger) (*Extractor, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}

	if logger == nil {
		return nil, errors.New("logger is nil")
	}

	return &Extractor{
		mutex:          &sync.Mutex{},
		client:         client,
		urnToUpstreams: make(map[string][]Resource),
		logger:         logger,
	}, nil
}

func (e *Extractor) ExtractUpstreams(ctx context.Context, query string, resourcesToIgnore []Resource) ([]Resource, error) {
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
) ([]Resource, error) {
	resources := parseFn(query)
	uniqueResources := UniqueFilterResources(resources)
	filteredResources := FilterResources(uniqueResources, func(r Resource) bool { return ignoredResources[r] })

	output := filteredResources
	var errorMessages []string

	resourceGroups := GroupResources(filteredResources)
	for _, group := range resourceGroups {
		result, err := e.getUpstreamsFromGroup(ctx, group, ignoredResources, encounteredResources)
		if err != nil {
			errorMessages = append(errorMessages, err.Error())
		}

		output = append(output, result...)
	}

	output = UniqueFilterResources(output)

	if len(errorMessages) > 0 {
		return output, fmt.Errorf("error reading upstream: [%s]", strings.Join(errorMessages, ", "))
	}
	return output, nil
}

func (e *Extractor) getUpstreamsFromGroup(
	ctx context.Context, group *ResourceGroup,
	ignoredResources, encounteredResources map[Resource]bool,
) ([]Resource, error) {
	var output []Resource
	var errorMessages []string

	schemas, err := ReadSchemasUnderGroup(ctx, e.client, group)
	if err != nil {
		if e.isIgnorableError(err) {
			e.logger.Error("ignoring error when reading schema for [%s.%s]: %v", group.Project, group.Dataset, err)
		} else {
			errorMessages = append(errorMessages, err.Error())
		}
	}

	nestedable, unnestedable := splitNestedableFromRest(schemas)

	unnestedableResources := convertSchemasToResources(unnestedable)
	output = append(output, unnestedableResources...)

	nestedResources, err := e.extractNestedableUpstreams(ctx, nestedable, ignoredResources, encounteredResources)
	if err != nil {
		errorMessages = append(errorMessages, err.Error())
	}

	output = append(output, nestedResources...)
	if len(errorMessages) > 0 {
		return output, fmt.Errorf("error getting upstream for [%s.%s]: %s", group.Project, group.Dataset, strings.Join(errorMessages, ", "))
	}

	return output, nil
}

func (e *Extractor) extractNestedableUpstreams(
	ctx context.Context, schemas []*Schema,
	ignoredResources, encounteredResources map[Resource]bool,
) ([]Resource, error) {
	var output []Resource
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

		output = append(output, sch.Resource)
		output = append(output, upstreams...)
	}

	if len(errorMessages) > 0 {
		return output, fmt.Errorf("error getting nested upstream: [%s]", strings.Join(errorMessages, ", "))
	}
	return output, nil
}

func (e *Extractor) getUpstreams(
	ctx context.Context, schema *Schema,
	ignoredResources, encounteredResources map[Resource]bool,
) ([]Resource, error) {
	urn := schema.Resource.URN()

	e.mutex.Lock()
	existingUpstreams, ok := e.urnToUpstreams[urn]
	e.mutex.Unlock()

	if ok {
		return existingUpstreams, nil
	}

	upstreams, err := e.extractUpstreamsFromQuery(ctx, schema.DDL, ignoredResources, encounteredResources, ParseNestedUpsreamsFromDDL)

	e.mutex.Lock()
	e.urnToUpstreams[urn] = upstreams
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

func (*Extractor) isIgnorableError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "access denied") || strings.Contains(msg, "user does not have permission")
}
