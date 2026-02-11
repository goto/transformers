package main

import (
	"context"
	e "errors"
	"fmt"
	"log/slog"
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
	ctx, cancelFn := signalAwareContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelFn()

	// initiate client
	c, err := client.NewClient(
		ctx,
		client.SetupLogger(l),
		client.SetupOTelSDK(cfg.OtelCollectorGRPCEndpoint, cfg.OtelAttributes),
		client.SetupODPSClient(cfg.GenOdps()),
		client.SetupDefaultProject(cfg.ExecutionProject),
		client.SetUpLogViewRetentionInDays(cfg.LogViewRetentionInDays),
		client.SetupDryRun(cfg.DryRun),
		client.SetupRetry(cfg.RetryMax, cfg.RetryBackoffMs),
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
			query.WithOverridedValue("_partitiontime", fmt.Sprintf("timestamp('%s')", dstart)),
			query.WithOverridedValue("_partitiondate", fmt.Sprintf("DATE(timestamp('%s'))", dstart)),
			query.WithAutoPartition(cfg.DevEnableAutoPartition == "true"),
			query.WithPartitionValue(cfg.DevEnablePartitionValue == "true"),
			query.WithCostAttributionLabel(cfg.CostAttributionTeam),
			query.WithColumnOrder(),
			query.WithDryRun(cfg.DryRun),
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
			query.WithCostAttributionLabel(cfg.CostAttributionTeam),
			query.WithColumnOrder(),
			query.WithDryRun(cfg.DryRun),
		)

		// -- TODO(START): refactor this part --
		// if multi query generation is disabled, then execute the query as is
		if cfg.DisableMultiQueryGeneration {
			queryToExecute, err := queryBuilder.SetOptions(
				query.WithQuery(string(raw)),
				query.WithOverridedValue("_partitiontime", fmt.Sprintf("timestamp('%s')", dstart)),
				query.WithOverridedValue("_partitiondate", fmt.Sprintf("DATE(timestamp('%s'))", dstart)),
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
				query.WithOverridedValue("_partitiontime", fmt.Sprintf("timestamp('%s')", dates[i])),
				query.WithOverridedValue("_partitiondate", fmt.Sprintf("DATE(timestamp('%s'))", dates[i])),
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
			query.WithCostAttributionLabel(cfg.CostAttributionTeam),
			query.WithMethod(query.MERGE),
			query.WithDryRun(cfg.DryRun),
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
		return executeConcurrently(ctx, l, c, cfg.Concurrency, queriesToExecute, cfg.AdditionalHints)
	}
	// otherwise execute sequentially
	return execute(ctx, l, c, queriesToExecute, cfg.AdditionalHints)
}

func executeConcurrently(ctx context.Context, l *slog.Logger, c *client.Client, concurrency int, queriesToExecute []string, additionalHints map[string]string) error {
	// execute query concurrently
	sem := make(chan uint8, concurrency)
	wg := sync.WaitGroup{}
	wg.Add(len(queriesToExecute))
	errChan := make(chan error, len(queriesToExecute))
	ids := sync.Map{} // id to boolean map to track running ids

	for i, queryToExecute := range queriesToExecute {
		sem <- 0
		id := i + 1
		ids.Store(id, false)
		executeFn := c.ExecuteFn(id)
		go func(id int, queryToExecute string, errChan chan error) {
			defer func() {
				wg.Done()
				<-sem
				ids.Delete(id)
				// logs the remaining running ids
				remainingIds := []int{}
				ids.Range(func(key, value any) bool {
					remainingIds = append(remainingIds, key.(int))
					return true
				})
				if len(remainingIds) > 0 {
					l.Info(fmt.Sprintf("remaining running ids: %v", remainingIds))
					l.Info(fmt.Sprintf("waiting for %d other queries to finish...", len(remainingIds)))
				}
			}()
			err := executeFn(ctx, queryToExecute, additionalHints)
			if err != nil {
				errChan <- errors.WithStack(err)
			}
		}(id, queryToExecute, errChan)
	}

	wg.Wait()
	close(errChan)

	l.Info("all queries have been processed")

	// check error
	var errs error
	for err := range errChan {
		if err != nil {
			errs = e.Join(errs, err)
		}
	}
	return errs
}

func execute(ctx context.Context, l *slog.Logger, c *client.Client, queriesToExecute []string, additionalHints map[string]string) error {
	for i, queryToExecute := range queriesToExecute {
		l.Info(fmt.Sprintf("processing query %d of %d", i+1, len(queriesToExecute)))
		executeFn := c.ExecuteFn(i + 1)
		err := executeFn(ctx, queryToExecute, additionalHints)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	l.Info("all queries have been processed")
	return nil
}

// signalAwareContext creates a context that is aware of signals.
func signalAwareContext(parent context.Context, signals ...os.Signal) (context.Context, context.CancelFunc) {
	ctx, cancelWithCause := context.WithCancelCause(parent)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, signals...)

	// start a goroutine to handle signals
	go func() {
		select {
		case sig := <-sigCh:
			cancelWithCause(fmt.Errorf("signal: %v", sig))
			signal.Stop(sigCh)
		case <-ctx.Done():
			signal.Stop(sigCh)
		}
	}()

	// return a standard CancelFunc that preserves the original behavior
	return ctx, func() {
		cancelWithCause(context.Canceled)
	}
}
