package client

import (
	"log/slog"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/pkg/errors"
)

type SetupFn func(c *Client) error

func SetUpLogViewRetentionInDays(days int) SetupFn {
	return func(c *Client) error {
		if c.OdpsClient == nil {
			return errors.New("odps client is required")
		}
		if days > 0 {
			c.OdpsClient.SetLogViewRetentionInDays(days)
		}
		return nil
	}
}

func SetupDefaultProject(project string) SetupFn {
	return func(c *Client) error {
		if c.OdpsClient == nil {
			return errors.New("odps client is required")
		}
		if project == "" {
			return nil
		}
		c.OdpsClient.SetDefaultProject(project)
		return nil
	}
}

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

func SetupOTelSDK(collectorGRPCEndpoint string, otelAttributes string) SetupFn {
	return func(c *Client) error {
		if collectorGRPCEndpoint == "" {
			return nil
		}
		attrSlice := strings.Split(otelAttributes, ",")
		attr := make(map[string]string, len(attrSlice))
		for _, a := range attrSlice {
			kv := strings.Split(a, "=")
			if len(kv) == 2 {
				attr[kv[0]] = kv[1]
			}
		}
		shutdownFn, err := setupOTelSDK(c.appCtx, collectorGRPCEndpoint, attr)
		if err != nil {
			return errors.WithStack(err)
		}
		c.shutdownFns = append(c.shutdownFns, shutdownFn)
		return nil
	}
}
