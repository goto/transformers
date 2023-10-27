package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/hashicorp/go-hclog"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v2"
	"google.golang.org/api/option"
	storageV1 "google.golang.org/api/storage/v1"

	"github.com/goto/transformers/task/bq2bq/upstream"
)

type DefaultBQClientFactory struct {
}

func (fac *DefaultBQClientFactory) New(ctx context.Context, svcAccount string) (bqiface.Client, error) {
	cred, err := google.CredentialsFromJSON(ctx, []byte(svcAccount),
		bigquery.Scope, storageV1.CloudPlatformScope, drive.DriveScope)
	if err != nil {
		return nil, fmt.Errorf("failed to read secret: %w", err)
	}

	client, err := bigquery.NewClient(ctx, cred.ProjectID, option.WithCredentials(cred))
	if err != nil {
		return nil, fmt.Errorf("failed to create BQ client: %w", err)
	}

	return bqiface.AdaptClient(client), nil
}

type DefaultUpstreamExtractorFactory struct {
}

func (d *DefaultUpstreamExtractorFactory) New(client bqiface.Client, logger hclog.Logger) (UpstreamExtractor, error) {
	return upstream.NewExtractor(client, logger)
}
