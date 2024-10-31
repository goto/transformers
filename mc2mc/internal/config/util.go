package config

import (
	"os"
	"strings"
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

// specific to parse job name from JOB_LABELS environment variable
// later on this should be refactored properly
func getJobName() string {
	return parseLabels("job_name")
}

// specific parsing for JOB_LABELS environment variable
// later on this should be refactored properly
func parseLabels(key string) string {
	labels := strings.Split(getEnv("JOB_LABELS", ""), ",")
	// parse label from JOB_LABELS based on key
	for _, label := range labels {
		parsed := strings.Split(label, "=")
		if len(parsed) == 2 && parsed[0] == key {
			return parsed[1]
		}
	}
	return ""
}
