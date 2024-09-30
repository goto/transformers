package main

import (
	"fmt"
	"os"

	_ "github.com/aliyun/aliyun-odps-go-sdk/sqldriver"
)

func main() {
	// max2max is the main function to execute the max2max transformation
	if err := max2max(); err != nil {
		fmt.Printf("error: %+v\n", err)
		os.Exit(1)
	}
}
