package main

import (
	"os"

	"github.com/aereal/dynamodb-export-poller/cli"
)

func main() {
	app := &cli.App{}
	os.Exit(app.Run(os.Args))
}
