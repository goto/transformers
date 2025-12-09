package client

import (
	"context"
	e "errors"
	"fmt"
	"log/slog"

	"github.com/pkg/errors"
)

const (
	SqlScriptSequenceHint = "goto.sql.script.sequence"
)

type OdpsClient interface {
	ExecSQL(ctx context.Context, query string, hints map[string]string) error
	SetDefaultProject(project string)
	SetLogViewRetentionInDays(days int)
	SetDryRun(dryRun bool)
	SetRetry(max int, backoffMs int)
	SetPriority(priority int)
}

type Client struct {
	OdpsClient OdpsClient

	appCtx      context.Context
	logger      *slog.Logger
	shutdownFns []func() error
}

func NewClient(ctx context.Context, setupFns ...SetupFn) (*Client, error) {
	c := &Client{
		appCtx:      ctx,
		shutdownFns: make([]func() error, 0),
	}
	for _, setupFn := range setupFns {
		if err := setupFn(c); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return c, nil
}

func (c *Client) Close() error {
	c.logger.Info("closing client")
	var err error
	for _, fn := range c.shutdownFns {
		err = e.Join(err, fn())
	}
	return errors.WithStack(err)
}

func (c *Client) ExecuteFn(id int) func(context.Context, string, map[string]string) error {
	return func(ctx context.Context, query string, additionalHints map[string]string) error {
		c.logger.Info(fmt.Sprintf("[sequence: %d] query to execute:\n%s", id, query))
		// Create local copy of additionalHints with sequence hint
		hints := make(map[string]string, len(additionalHints)+1)
		for k, v := range additionalHints {
			hints[k] = v
		}
		hints[SqlScriptSequenceHint] = fmt.Sprintf("%d", id)

		// execute query with odps client
		if err := c.OdpsClient.ExecSQL(ctx, query, hints); err != nil {
			return errors.WithStack(err)
		}

		c.logger.Info(fmt.Sprintf("[sequence: %d] execution done", id))
		return nil
	}
}
