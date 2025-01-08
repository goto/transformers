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
		queryBuilder := query.NewBuilder(
			l,
			client.NewODPSClient(l, cfg.GenOdps()),
			query.WithMethod(query.REPLACE),
			query.WithDestination(cfg.DestinationTableID),
			query.WithAutoPartition(cfg.DevEnableAutoPartition == "true"),
			query.WithPartitionValue(cfg.DevEnablePartitionValue == "true"),
			query.WithColumnOrder(),
		)

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
		queriesToExecute = append(queriesToExecute, queryToExecute)
	default:
		return errors.Errorf("not supported load method: %s", cfg.LoadMethod)
	}

	// execute query concurrently
	wg := sync.WaitGroup{}
	wg.Add(len(queriesToExecute))
	errChan := make(chan error, len(queriesToExecute))

	for _, queryToExecute := range queriesToExecute {
		go func(queryToExecute string, errChan chan error) {
			err := c.Execute(ctx, queryToExecute)
			if err != nil {
				errChan <- errors.WithStack(err)
			}
			wg.Done()
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
