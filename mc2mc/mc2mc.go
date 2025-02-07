package main

import (
	"context"
	e "errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"

	"github.com/goto/transformers/mc2mc/internal/client"
	"github.com/goto/transformers/mc2mc/internal/config"
	"github.com/goto/transformers/mc2mc/internal/logger"
	"github.com/goto/transformers/mc2mc/internal/query"
)

func mc2mc(envs []string) error {
	// load config
	cfg, err := config.NewConfig(envs...)
	if err != nil {
		return errors.WithStack(err)
	}

	// set up logger
	l, err := logger.NewLogger(cfg.LogLevel)
	if err != nil {
		return errors.WithStack(err)
	}

	// graceful shutdown
	ctx, cancelFn := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelFn()

	// initiate client
	c, err := client.NewClient(
		ctx,
		client.SetupLogger(l),
		client.SetupOTelSDK(cfg.OtelCollectorGRPCEndpoint, cfg.OtelAttributes),
		client.SetupODPSClient(cfg.GenOdps()),
		client.SetupDefaultProject(cfg.ExecutionProject),
		client.SetUpLogViewRetentionInDays(cfg.LogViewRetentionInDays),
	)
	if err != nil {
		return errors.WithStack(err)
	}
	defer c.Close()

	// parse date range
	start, err := time.Parse(time.RFC3339, cfg.DStart)
	if err != nil {
		return errors.WithStack(err)
	}
	end, err := time.Parse(time.RFC3339, cfg.DEnd)
	if err != nil {
		return errors.WithStack(err)
	}

	// build query
	raw, err := os.ReadFile(cfg.QueryFilePath)
	if err != nil {
		return errors.WithStack(err)
	}
	queriesToExecute := []string{}
	switch cfg.LoadMethod {
	case "APPEND":
		dstart := start.Format(time.DateTime) // normalize date format as temporary support
		queryToExecute, err := query.NewBuilder(
			l,
			client.NewODPSClient(l, cfg.GenOdps()),
			query.WithQuery(string(raw)),
			query.WithMethod(query.APPEND),
			query.WithDestination(cfg.DestinationTableID),
			query.WithOverridedValue("_partitiontime", fmt.Sprintf("TIMESTAMP('%s')", dstart)),
			query.WithOverridedValue("_partitiondate", fmt.Sprintf("DATE(TIMESTAMP('%s'))", dstart)),
			query.WithAutoPartition(cfg.DevEnableAutoPartition == "true"),
			query.WithPartitionValue(cfg.DevEnablePartitionValue == "true"),
			query.WithColumnOrder(),
		).Build()
		if err != nil {
			return errors.WithStack(err)
		}
		queriesToExecute = append(queriesToExecute, queryToExecute)
	case "REPLACE":
		dstart := start.Format(time.DateTime) // normalize date format as temporary support
		queryBuilder := query.NewBuilder(
			l,
			client.NewODPSClient(l, cfg.GenOdps()),
			query.WithMethod(query.REPLACE),
			query.WithDestination(cfg.DestinationTableID),
			query.WithAutoPartition(cfg.DevEnableAutoPartition == "true"),
			query.WithPartitionValue(cfg.DevEnablePartitionValue == "true"),
			query.WithColumnOrder(),
		)

		// -- TODO(START): refactor this part --
		// if multi query generation is disabled, then execute the query as is
		if cfg.DisableMultiQueryGeneration {
			queryToExecute, err := queryBuilder.SetOptions(
				query.WithQuery(string(raw)),
				query.WithOverridedValue("_partitiontime", fmt.Sprintf("TIMESTAMP('%s')", dstart)),
				query.WithOverridedValue("_partitiondate", fmt.Sprintf("DATE(TIMESTAMP('%s'))", dstart)),
			).Build()
			if err != nil {
				return errors.WithStack(err)
			}
			queriesToExecute = append(queriesToExecute, queryToExecute)
			break
		}

		// generate queries for each date
		// if it contains break marker, it must uses window range greater than 1 day
		// if table destination is partition table, then it will be replaced based on the partition date
		// for non partition table, only last query will be applied
		queries := strings.Split(string(raw), query.BREAK_MARKER)
		dates := []string{}
		if end.Sub(start) <= time.Hour*24 { // if window size is less than or equal to partition delta(a DAY), then uses the same date
			dates = append(dates, start.Format(time.DateTime)) // normalize date format as temporary support
		} else { // otherwise, generate dates
			for i := start; i.Before(end); i = i.AddDate(0, 0, 1) {
				dates = append(dates, i.Format(time.DateTime)) // normalize date format as temporary support
			}
		}

		if len(queries) != len(dates) {
			return errors.Errorf("number of generated queries and dates are not matched: %d != %d", len(queries), len(dates))
		}

		for i, currentQueryToExecute := range queries {
			currentQueryBuilder := queryBuilder
			queryToExecute, err := currentQueryBuilder.SetOptions(
				query.WithQuery(currentQueryToExecute),
				query.WithOverridedValue("_partitiontime", fmt.Sprintf("TIMESTAMP('%s')", dates[i])),
				query.WithOverridedValue("_partitiondate", fmt.Sprintf("DATE(TIMESTAMP('%s'))", dates[i])),
			).Build()
			if err != nil {
				return errors.WithStack(err)
			}
			queriesToExecute = append(queriesToExecute, queryToExecute)
		}
		// -- TODO(END): refactor this part --
	case "MERGE":
		queryToExecute, err := query.NewBuilder(
			l,
			client.NewODPSClient(l, cfg.GenOdps()),
			query.WithQuery(string(raw)),
			query.WithMethod(query.MERGE),
		).Build()
		if err != nil {
			return errors.WithStack(err)
		}
		queriesToExecute = append(queriesToExecute, strings.Split(queryToExecute, query.BREAK_MARKER)...)
	default:
		return errors.Errorf("not supported load method: %s", cfg.LoadMethod)
	}

	// only support concurrent execution for REPLACE method
	if cfg.LoadMethod == "REPLACE" {
		return executeConcurrently(ctx, c, cfg.Concurrency, queriesToExecute)
	}
	// otherwise execute sequentially
	return execute(ctx, c, queriesToExecute)
}

func executeConcurrently(ctx context.Context, c *client.Client, concurrency int, queriesToExecute []string) error {
	// execute query concurrently
	sem := make(chan uint8, concurrency)
	wg := sync.WaitGroup{}
	wg.Add(len(queriesToExecute))
	errChan := make(chan error, len(queriesToExecute))

	for _, queryToExecute := range queriesToExecute {
		sem <- 0
		go func(queryToExecute string, errChan chan error) {
			err := c.Execute(ctx, queryToExecute)
			if err != nil {
				errChan <- errors.WithStack(err)
			}
			wg.Done()
			<-sem
		}(queryToExecute, errChan)
	}

	wg.Wait()
	close(errChan)

	// check error
	var errs error
	for err := range errChan {
		if err != nil {
			errs = e.Join(errs, err)
		}
	}
	return errs
}

func execute(ctx context.Context, c *client.Client, queriesToExecute []string) error {
	for _, queryToExecute := range queriesToExecute {
		err := c.Execute(ctx, queryToExecute)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
