package config

import (
	"encoding/json"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/pkg/errors"
)

type Config struct {
	*odps.Config
	LogLevel                  string
	LoadMethod                string
	QueryFilePath             string
	DestinationTableID        string
	OtelCollectorGRPCEndpoint string
	JobName                   string
	ScheduledTime             string
}

type maxComputeCredentials struct {
	AccessId    string `json:"access_id"`
	AccessKey   string `json:"access_key"`
	Endpoint    string `json:"endpoint"`
	ProjectName string `json:"project_name"`
}

func NewConfig() (*Config, error) {
	cfg := &Config{
		Config: odps.NewConfig(),
		// mc2mc related config
		LogLevel:           getEnv("LOG_LEVEL", "INFO"),
		LoadMethod:         getEnv("LOAD_METHOD", "APPEND"),
		QueryFilePath:      getEnv("QUERY_FILE_PATH", "/data/in/query.sql"),
		DestinationTableID: getEnv("DESTINATION_TABLE_ID", ""),
		// system related config
		OtelCollectorGRPCEndpoint: getEnv("OTEL_COLLECTOR_GRPC_ENDPOINT", ""),
		JobName:                   getJobName(),
		ScheduledTime:             getEnv("SCHEDULED_TIME", ""),
	}
	// ali-odps-go-sdk related config
	scvAcc := getEnv("SERVICE_ACCOUNT", "")
	cred, err := collectMaxComputeCredential([]byte(scvAcc))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cfg.Config.AccessId = cred.AccessId
	cfg.Config.AccessKey = cred.AccessKey
	cfg.Config.Endpoint = cred.Endpoint
	cfg.Config.ProjectName = cred.ProjectName
	cfg.Config.HttpTimeout = getEnvDuration("MAXCOMPUTE_HTTP_TIMEOUT", "10s")
	cfg.Config.TcpConnectionTimeout = getEnvDuration("MAXCOMPUTE_TCP_TIMEOUT", "30s")

	return cfg, nil
}

func collectMaxComputeCredential(scvAcc []byte) (*maxComputeCredentials, error) {
	var creds maxComputeCredentials
	if err := json.Unmarshal(scvAcc, &creds); err != nil {
		return nil, errors.WithStack(err)
	}

	return &creds, nil
}
