package config

import (
	"encoding/json"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/pkg/errors"
)

// ConfigEnv is a mc configuration for the component.
type ConfigEnv struct {
	LogLevel                    string            `env:"LOG_LEVEL" envDefault:"INFO"`
	OtelCollectorGRPCEndpoint   string            `env:"OTEL_COLLECTOR_GRPC_ENDPOINT"`
	OtelAttributes              string            `env:"OTEL_ATTRIBUTES"`
	MCServiceAccount            string            `env:"MC_SERVICE_ACCOUNT"`
	LoadMethod                  string            `env:"LOAD_METHOD" envDefault:"APPEND"`
	QueryFilePath               string            `env:"QUERY_FILE_PATH" envDefault:"/data/in/query.sql"`
	DestinationTableID          string            `env:"DESTINATION_TABLE_ID"`
	DStart                      string            `env:"DSTART"`
	DEnd                        string            `env:"DEND"`
	ExecutionProject            string            `env:"EXECUTION_PROJECT"`
	Concurrency                 int               `env:"CONCURRENCY" envDefault:"7"`
	AdditionalHints             map[string]string `env:"ADDITIONAL_HINTS" envKeyValSeparator:"=" envSeparator:","`
	LogViewRetentionInDays      int               `env:"LOG_VIEW_RETENTION_IN_DAYS" envDefault:"2"`
	DisableMultiQueryGeneration bool              `env:"DISABLE_MULTI_QUERY_GENERATION" envDefault:"false"`
	DryRun                      bool              `env:"DRY_RUN" envDefault:"false"`
	RetryMax                    int               `env:"RETRY_MAX" envDefault:"3"`
	RetryBackoffMs              int               `env:"RETRY_BACKOFF_MS" envDefault:"1000"`
	// TODO: delete this
	DevEnablePartitionValue string `env:"DEV__ENABLE_PARTITION_VALUE" envDefault:"false"`
	DevEnableAutoPartition  string `env:"DEV__ENABLE_AUTO_PARTITION" envDefault:"false"`
}

type Config struct {
	*odps.Config
	*ConfigEnv
}

// NewConfig parses the environment variables and returns the mc configuration.
func NewConfig(envs ...string) (*Config, error) {
	configEnv, err := parse[ConfigEnv](envs...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cred, err := collectMaxComputeCredential([]byte(configEnv.MCServiceAccount))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cfg := &Config{
		Config:    &odps.Config{},
		ConfigEnv: configEnv,
	}
	cfg.Config.AccessId = cred.AccessId
	cfg.Config.AccessKey = cred.AccessKey
	cfg.Config.Endpoint = cred.Endpoint
	cfg.Config.ProjectName = cred.ProjectName

	return cfg, nil
}

type maxComputeCredentials struct {
	AccessId    string `json:"access_id"`
	AccessKey   string `json:"access_key"`
	Endpoint    string `json:"endpoint"`
	ProjectName string `json:"project_name"`
}

func collectMaxComputeCredential(scvAcc []byte) (*maxComputeCredentials, error) {
	var creds maxComputeCredentials
	if err := json.Unmarshal(scvAcc, &creds); err != nil {
		return nil, errors.WithStack(err)
	}

	return &creds, nil
}
