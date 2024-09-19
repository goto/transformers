package config

import (
	"os"
	"time"
)

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvDuration(key, fallback string) time.Duration {
	result, _ := time.ParseDuration(getEnv(key, fallback))
	return result
}
