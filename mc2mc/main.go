package main

import (
	"fmt"
	"os"

	_ "github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
	"github.com/spf13/pflag"

	"github.com/goto/transformers/mc2mc/internal/logger"
)

func main() {
	// initiate default logger
	l := logger.NewDefaultLogger()

	// Parse the flags.
	var envs []string
	pflag.StringArrayVar(&envs, "env", []string{}, "pass env as argument (can be used multiple times)")
	pflag.Parse()

	// mc2mc is the main function to execute the mc2mc transformation
	// which reads the configuration, sets up the client and executes the query.
	// It also handles graceful shutdown by listening to os signals.
	// It returns error if any.
	if err := mc2mc(envs); err != nil {
		l.Error(fmt.Sprintf("error: %s", err.Error()))
		fmt.Printf("error: %+v\n", err)
		os.Exit(1)
	}
}
