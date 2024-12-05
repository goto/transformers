package client

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/pkg/errors"

	"github.com/goto/transformers/mc2mc/internal/loader"
	"github.com/goto/transformers/mc2mc/internal/logger"
)

type SetupFn func(c *Client) error

func SetupLogger(logLevel string) SetupFn {
	return func(c *Client) error {
		logger, err := logger.NewLogger(logLevel)
		if err != nil {
			return errors.WithStack(err)
		}
		c.logger = logger
		return nil
	}
}

func SetupODPSClient(odpsClient *odps.Odps) SetupFn {
	return func(c *Client) error {
		c.OdpsClient = NewODPSClient(c.logger, odpsClient)
		return nil
	}
}

func SetupOTelSDK(collectorGRPCEndpoint, jobName, scheduledTime string) SetupFn {
	return func(c *Client) error {
		if collectorGRPCEndpoint == "" {
			return nil
		}
		shutdownFn, err := setupOTelSDK(c.appCtx, collectorGRPCEndpoint, jobName, scheduledTime)
		if err != nil {
			return errors.WithStack(err)
		}
		c.shutdownFns = append(c.shutdownFns, shutdownFn)
		return nil
	}
}

func SetupLoader(loadMethod string) SetupFn {
	return func(c *Client) error {
		loader, err := loader.GetLoader(loadMethod, c.logger)
		if err != nil {
			return errors.WithStack(err)
		}
		c.Loader = loader
		return nil
	}
}

func EnablePartitionValue(enabled bool) SetupFn {
	return func(c *Client) error {
		c.enablePartitionValue = enabled
		return nil
	}
}
