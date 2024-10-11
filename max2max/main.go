package main

import (
	"fmt"
	"os"

	_ "github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
)

func main() {
	// max2max is the main function to execute the max2max transformation
	// which reads the configuration, sets up the client and executes the query.
	// It also handles graceful shutdown by listening to os signals.
	// It returns error if any.
	if err := max2max(); err != nil {
		fmt.Printf("error: %+v\n", err)
		os.Exit(1)
	}
}
