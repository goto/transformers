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
	OtelAttributes            string
	JobName                   string
	ScheduledTime             string
	DStart                    string
	// TODO: remove this temporary support after 15 nov 2024
	DevEnablePartitionValue bool
	DevEnableAutoPartition  bool
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
		OtelAttributes:            getEnv("OTEL_ATTRIBUTES", ""),
		JobName:                   getJobName(),
		ScheduledTime:             getEnv("SCHEDULED_TIME", ""),
		DStart:                    getEnv("DSTART", ""),
		// TODO: delete this after 15 nov
		DevEnablePartitionValue: getEnv("DEV__ENABLE_PARTITION_VALUE", "false") == "true",
		DevEnableAutoPartition:  getEnv("DEV__ENABLE_AUTO_PARTITION", "false") == "true",
	}
	// ali-odps-go-sdk related config
	scvAcc := getEnv("MC_SERVICE_ACCOUNT", "")
	cred, err := collectMaxComputeCredential([]byte(scvAcc))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cfg.Config.AccessId = cred.AccessId
	cfg.Config.AccessKey = cred.AccessKey
	cfg.Config.Endpoint = cred.Endpoint
	cfg.Config.ProjectName = cred.ProjectName
	cfg.Config.HttpTimeout = getEnvDuration("MC_HTTP_TIMEOUT", "10s")
	cfg.Config.TcpConnectionTimeout = getEnvDuration("MC_TCP_TIMEOUT", "30s")

	return cfg, nil
}

func collectMaxComputeCredential(scvAcc []byte) (*maxComputeCredentials, error) {
	var creds maxComputeCredentials
	if err := json.Unmarshal(scvAcc, &creds); err != nil {
		return nil, errors.WithStack(err)
	}

	return &creds, nil
}
