package client

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/goto/transformers/max2max/internal/logger"
)

type SetupFn func(c *Client) error

func SetupLogger(logLevel string) SetupFn {
	return func(c *Client) error {
		logger, err := logger.NewLogger(logLevel)
		if err != nil {
			return err
		}
		c.logger = logger
		return nil
	}
}

func SetupODPSClient(odpsClient *odps.Odps) SetupFn {
	return func(c *Client) error {
		c.OdpsClient = NewODPSClient(odpsClient)
		return nil
	}
}

func SetupOTelSDK(collectorGRPCEndpoint string) SetupFn {
	return func(c *Client) error {
		if collectorGRPCEndpoint == "" {
			return nil
		}
		shutdownFn, err := setupOTelSDK(collectorGRPCEndpoint)
		if err != nil {
			return err
		}
		c.shutdownFns = append(c.shutdownFns, shutdownFn)
		return nil
	}
}
