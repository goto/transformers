package upstream_test

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/iterator"

	"github.com/goto/transformers/task/bq2bq/upstream"
)

func TestNewExtractor(t *testing.T) {
	t.Run("should return nil and error if client is nil", func(t *testing.T) {
		var client bqiface.Client
		logger := hclog.NewNullLogger()

		actualExtractor, actualError := upstream.NewExtractor(client, logger)

		assert.Nil(t, actualExtractor)
		assert.EqualError(t, actualError, "client is nil")
	})

	t.Run("should return nil and error if logger is nil", func(t *testing.T) {
		client := new(ClientMock)
		var logger hclog.Logger

		actualExtractor, actualError := upstream.NewExtractor(client, logger)

		assert.Nil(t, actualExtractor)
		assert.EqualError(t, actualError, "logger is nil")
	})

	t.Run("should return extractor and nil if no error is encountered", func(t *testing.T) {
		client := new(ClientMock)
		logger := hclog.NewNullLogger()

		actualExtractor, actualError := upstream.NewExtractor(client, logger)

		assert.NotNil(t, actualExtractor)
		assert.NoError(t, actualError)
	})
}

func TestExtractor(t *testing.T) {
	logger := hclog.NewNullLogger()

	t.Run("ExtractUpstreams", func(t *testing.T) {
		t.Run("should pass the existing spec", func(t *testing.T) {
			testCases := []struct {
				Message           string
				QueryRequest      string
				ExpectedUpstreams []upstream.Resource
			}{
				{
					Message:      "should return upstreams and generate dependencies for select statements",
					QueryRequest: "Select * from proj.dataset.table1",
					ExpectedUpstreams: []upstream.Resource{
						{
							Project: "proj",
							Dataset: "dataset",
							Name:    "table1",
						},
					},
				},
				{
					Message:      "should return unique upstreams and nil for select statements",
					QueryRequest: "Select * from proj.dataset.table1 t1 join proj.dataset.table1 t2 on t1.col1 = t2.col1",
					ExpectedUpstreams: []upstream.Resource{
						{
							Project: "proj",
							Dataset: "dataset",
							Name:    "table1",
						},
					},
				},
				{
					Message:           "should return filtered upstreams for select statements with ignore statement",
					QueryRequest:      "Select * from /* @ignoreupstream */ proj.dataset.table1",
					ExpectedUpstreams: nil,
				},
				{
					Message:      "should return filtered upstreams for select statements with ignore statement for view",
					QueryRequest: "Select * from proj.dataset.table1 t1 join proj.dataset.table1 t2 on t1.col1 = t2.col1",
					ExpectedUpstreams: []upstream.Resource{
						{
							Project: "proj",
							Dataset: "dataset",
							Name:    "table1",
						},
					},
				},
			}

			for _, test := range testCases {
				client := new(ClientMock)
				query := new(QueryMock)
				rowIterator := new(RowIteratorMock)
				resourcestoIgnore := []upstream.Resource{
					{
						Project: "proj",
						Dataset: "datas",
						Name:    "tab",
					},
				}

				extractor, err := upstream.NewExtractor(client, logger)
				assert.NotNil(t, extractor)
				assert.NoError(t, err)

				ctx := context.Background()

				client.On("Query", mock.Anything).Return(query)

				query.On("Read", mock.Anything).Return(rowIterator, nil)

				rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
					v := args.Get(0).(*[]bigquery.Value)
					*v = []bigquery.Value{"proj", "dataset", "table1", "BASE TABLE", "select 1;"}
				}).Return(nil).Once()
				rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()

				actualUpstreams, actualError := extractor.ExtractUpstreams(ctx, test.QueryRequest, resourcestoIgnore)

				assert.EqualValues(t, test.ExpectedUpstreams, actualUpstreams, test.Message)
				assert.NoError(t, actualError, test.Message)
			}
		})

		t.Run("should return unique upstreams with its nested ones if found any", func(t *testing.T) {
			client := new(ClientMock)
			query := new(QueryMock)
			rowIterator := new(RowIteratorMock)
			resourcestoIgnore := []upstream.Resource{
				{
					Project: "project_test_0",
					Dataset: "dataset_test_0",
					Name:    "name_test_0",
				},
			}

			extractor, err := upstream.NewExtractor(client, logger)
			assert.NotNil(t, extractor)
			assert.NoError(t, err)

			ctx := context.Background()
			queryRequest := "select * from `project_test_1.dataset_test_1.name_test_1`"

			client.On("Query", mock.Anything).Return(query)

			query.On("Read", mock.Anything).Return(rowIterator, nil)

			rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
				v := args.Get(0).(*[]bigquery.Value)
				*v = []bigquery.Value{"project_test_1", "dataset_test_1", "name_test_1", "BASE TABLE", "select 1;"}
			}).Return(nil).Once()
			rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
				v := args.Get(0).(*[]bigquery.Value)
				*v = []bigquery.Value{"project_test_2", "dataset_test_2", "name_test_2", "VIEW", "select * from project_test_3.dataset_test_3.name_test_3;"}
			}).Return(nil).Once()
			rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()
			rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
				v := args.Get(0).(*[]bigquery.Value)
				*v = []bigquery.Value{"project_test_3", "dataset_test_3", "name_test_3", "BASE TABLE", "select 1"}
			}).Return(nil).Once()
			rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()

			expectedUpstreams := []upstream.Resource{
				{
					Project: "project_test_1",
					Dataset: "dataset_test_1",
					Name:    "name_test_1",
				},
				{
					Project: "project_test_2",
					Dataset: "dataset_test_2",
					Name:    "name_test_2",
				},
				{
					Project: "project_test_3",
					Dataset: "dataset_test_3",
					Name:    "name_test_3",
				},
			}

			actualUpstreams, actualError := extractor.ExtractUpstreams(ctx, queryRequest, resourcestoIgnore)

			assert.ElementsMatch(t, expectedUpstreams, actualUpstreams)
			assert.NoError(t, actualError)
		})

		t.Run("should return upstreams and nil if error being encountered is related to access denied", func(t *testing.T) {
			client := new(ClientMock)
			query := new(QueryMock)
			resourcestoIgnore := []upstream.Resource{
				{
					Project: "project_test_0",
					Dataset: "dataset_test_0",
					Name:    "name_test_0",
				},
			}

			extractor, err := upstream.NewExtractor(client, logger)
			assert.NotNil(t, extractor)
			assert.NoError(t, err)

			ctx := context.Background()
			queryRequest := "select * from `project_test_1.dataset_test_1.name_test_1`"

			client.On("Query", mock.Anything).Return(query)

			query.On("Read", mock.Anything).Return(nil, errors.New("Access Denied"))

			expectedUpstreams := []upstream.Resource{
				{
					Project: "project_test_1",
					Dataset: "dataset_test_1",
					Name:    "name_test_1",
				},
			}

			actualUpstreams, actualError := extractor.ExtractUpstreams(ctx, queryRequest, resourcestoIgnore)

			assert.ElementsMatch(t, expectedUpstreams, actualUpstreams)
			assert.NoError(t, actualError)
		})

		t.Run("should return upstreams and nil if error being encountered is related to user does not have permission", func(t *testing.T) {
			client := new(ClientMock)
			query := new(QueryMock)
			resourcestoIgnore := []upstream.Resource{
				{
					Project: "project_test_0",
					Dataset: "dataset_test_0",
					Name:    "name_test_0",
				},
			}

			extractor, err := upstream.NewExtractor(client, logger)
			assert.NotNil(t, extractor)
			assert.NoError(t, err)

			ctx := context.Background()
			queryRequest := "select * from `project_test_1.dataset_test_1.name_test_1`"

			client.On("Query", mock.Anything).Return(query)

			query.On("Read", mock.Anything).Return(nil, errors.New("User does not have permission"))

			expectedUpstreams := []upstream.Resource{
				{
					Project: "project_test_1",
					Dataset: "dataset_test_1",
					Name:    "name_test_1",
				},
			}

			actualUpstreams, actualError := extractor.ExtractUpstreams(ctx, queryRequest, resourcestoIgnore)

			assert.ElementsMatch(t, expectedUpstreams, actualUpstreams)
			assert.NoError(t, actualError)
		})

		t.Run("should return error if circular reference is detected", func(t *testing.T) {
			client := new(ClientMock)
			query := new(QueryMock)
			rowIterator := new(RowIteratorMock)
			resourcestoIgnore := []upstream.Resource{}

			extractor, err := upstream.NewExtractor(client, logger)
			assert.NotNil(t, extractor)
			assert.NoError(t, err)

			ctx := context.Background()
			queryRequest := "select * from `project_test_1.dataset_test_1.cyclic_test_1`"

			client.On("Query", mock.Anything).Return(query)

			query.On("Read", mock.Anything).Return(rowIterator, nil)

			rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
				v := args.Get(0).(*[]bigquery.Value)
				*v = []bigquery.Value{"project_test_1", "dataset_test_1", "cyclic_test_1", "VIEW", "select * from project_test_3.dataset_test_3.cyclic_test_3"}
			}).Return(nil).Once()
			rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()
			rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
				v := args.Get(0).(*[]bigquery.Value)
				*v = []bigquery.Value{"project_test_3", "dataset_test_3", "cyclic_test_3", "VIEW", "select * from project_test_2.dataset_test_2.cyclic_test_2"}
			}).Return(nil).Once()
			rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()
			rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
				v := args.Get(0).(*[]bigquery.Value)
				*v = []bigquery.Value{"project_test_2", "dataset_test_2", "cyclic_test_2", "VIEW", "select * from project_test_1.dataset_test_1.cyclic_test_1"}
			}).Return(nil).Once()
			rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()
			rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
				v := args.Get(0).(*[]bigquery.Value)
				*v = []bigquery.Value{"project_test_1", "dataset_test_1", "cyclic_test_1", "VIEW", "select * from project_test_3.dataset_test_3.cyclic_test_3"}
			}).Return(nil).Once()
			rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()

			expectedUpstreams := []upstream.Resource{
				{

					Project: "project_test_1",
					Dataset: "dataset_test_1",
					Name:    "cyclic_test_1",
				},
				{
					Project: "project_test_2",
					Dataset: "dataset_test_2",
					Name:    "cyclic_test_2",
				},
				{
					Project: "project_test_3",
					Dataset: "dataset_test_3",
					Name:    "cyclic_test_3",
				},
			}

			actualUpstreams, actualError := extractor.ExtractUpstreams(ctx, queryRequest, resourcestoIgnore)

			assert.ElementsMatch(t, expectedUpstreams, actualUpstreams)
			assert.ErrorContains(t, actualError, "circular reference is detected")
		})
	})
}
