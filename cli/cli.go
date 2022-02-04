package cli

import (
	"context"
	"flag"
	"io"
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

var defaultWriter io.Writer

func NewApp(out io.Writer) *App {
	if out == nil {
		out = defaultWriter
	}
	return &App{out: out}
}

type App struct {
	out io.Writer
}

func (c *App) Run(argv []string) int {
	fls := flag.NewFlagSet(argv[0], flag.ContinueOnError)
	fls.SetOutput(c.out)
	log.Logger = log.Logger.Output(c.out)
	opts := ddbexportpoller.PollerOptions{}
	var (
		debug bool
	)
	fls.StringVar(&opts.TableArn, "table-arn", "", "table ARN to watch exports")
	fls.BoolVar(&debug, "debug", false, "debug mode")
	fls.DurationVar(&opts.InitialDelay, "initial-delay", time.Second, "initial wait time")
	fls.DurationVar(&opts.MaxDelay, "max-delay", time.Second*10, "max wait time")
	fls.Int64Var(&opts.Concurrency, "concurrency", int64(runtime.NumCPU()), "concurrency to run requests")
	fls.IntVar(&opts.MaxAttempts, "max-attempts", 0, "max attempts (zero means forever)")
	fls.DurationVar(&opts.Timeout, "timeout", 0, "global timeout (zero means waits forever)")
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
	poller, err := ddbexportpoller.NewPoller(opts)
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
