package cli

import (
	"context"
	"flag"

	ddbexportpoller "github.com/aereal/dynamodb-export-poller"
	"github.com/rs/zerolog/log"
)

const (
	statusOK int = iota
	statusNG
)

type App struct{}

func (c *App) Run(argv []string) int {
	fls := flag.NewFlagSet(argv[0], flag.ContinueOnError)
	var (
		tableArn string
	)
	fls.StringVar(&tableArn, "table-arn", "", "table ARN to watch exports")
	switch err := fls.Parse(argv[1:]); err {
	case nil: // continue
	case flag.ErrHelp:
		return statusOK
	default: // error but not ErrHelp
		log.Error().Err(err).Send()
		return statusNG
	}
	ctx := context.Background()
	if err := c.run(ctx, tableArn); err != nil {
		log.Error().Err(err).Send()
		return statusNG
	}
	return statusOK
}

func (c *App) run(ctx context.Context, tableArn string) error {
	poller, err := ddbexportpoller.NewPoller(tableArn)
	if err != nil {
		return err
	}
	return poller.PollExports(ctx)
}
