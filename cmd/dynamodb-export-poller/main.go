package main

import (
	"os"

	"github.com/aereal/dynamodb-export-poller/cli"
)

func main() {
	app := cli.NewApp(os.Stderr)
	os.Exit(app.Run(os.Args))
}
