package client

import (
	"log/slog"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/pkg/errors"
)

type SetupFn func(c *Client) error

func SetupLogger(logger *slog.Logger) SetupFn {
	return func(c *Client) error {
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
