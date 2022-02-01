package cli

import (
	"context"
	"flag"
	"runtime"
	"time"

	ddbexportpoller "github.com/aereal/dynamodb-export-poller"
	"github.com/rs/zerolog"
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
		tableArn     string
		debug        bool
		initialDelay time.Duration
		maxWorkers   int64
	)
	fls.StringVar(&tableArn, "table-arn", "", "table ARN to watch exports")
	fls.BoolVar(&debug, "debug", false, "debug mode")
	fls.DurationVar(&initialDelay, "initial-delay", time.Second, "initial wait time")
	fls.Int64Var(&maxWorkers, "max-workers", int64(runtime.NumCPU()), "max workers count to run requests")
	switch err := fls.Parse(argv[1:]); err {
	case nil: // continue
	case flag.ErrHelp:
		return statusOK
	default: // error but not ErrHelp
		log.Error().Err(err).Send()
		return statusNG
	}
	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	ctx := context.Background()
	poller, err := ddbexportpoller.NewPoller(tableArn, initialDelay, maxWorkers)
	if err != nil {
		log.Error().Err(err).Send()
		return statusNG
	}
	if err := poller.PollExports(ctx); err != nil {
		log.Error().Err(err).Send()
		return statusNG
	}
	return statusOK
}
