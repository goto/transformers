package config

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

type Config struct {
	*odps.Config
	LogLevel           string
	LoadMethod         string
	QueryFilePath      string
	DestinationTableID string
}

func NewConfig() *Config {
	cfg := &Config{
		Config: odps.NewConfig(),
		// max2max related config
		LogLevel:           getEnv("LOG_LEVEL", "INFO"),
		LoadMethod:         getEnv("LOAD_METHOD", "APPEND"),
		QueryFilePath:      getEnv("QUERY_FILE_PATH", ""),
		DestinationTableID: getEnv("DESTINATION_TABLE_ID", ""),
	}
	// ali-odps-go-sdk related config
	cfg.Config.AccessId = getEnv("ACCESS_ID", "")
	cfg.Config.AccessKey = getEnv("ACCESS_KEY", "")
	cfg.Config.Endpoint = getEnv("ENDPOINT", "http://service.ap-southeast-5.maxcompute.aliyun.com/api")
	cfg.Config.ProjectName = getEnv("PROJECT", "")
	cfg.Config.HttpTimeout = getEnvDuration("HTTP_TIMEOUT", "10s")

	return cfg
}
