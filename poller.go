package ddbexportpoller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/rs/zerolog/log"
	"github.com/shogo82148/go-retry"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var (
	ErrTableArnRequired = errors.New("table ARN required")

	errExportNotFinite = errors.New("export is not finite")
)

type PollerOptions struct {
	TableArn      string
	InitialDelay  time.Duration
	MaxDelay      time.Duration
	MaxAttempts   int
	Concurrency   int64
	GlobalTimeout time.Duration
}

func (o PollerOptions) validate() error {
	if o.TableArn == "" {
		return ErrTableArnRequired
	}
	return nil
}

var noop = func() {}

func (o PollerOptions) withTimeout(parent context.Context) (context.Context, func()) {
	if o.GlobalTimeout == 0 {
		return parent, noop
	}
	return context.WithTimeout(parent, o.GlobalTimeout)
}

func NewPoller(options PollerOptions) (*Poller, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("LoadDefaultConfig(): %w", err)
	}
	poller := &Poller{options: options}
	poller.client = dynamodb.NewFromConfig(cfg)
	return poller, nil
}

type Poller struct {
	options PollerOptions
	client  *dynamodb.Client
}

func (p *Poller) PollExports(ctx context.Context) error {
	out, err := p.client.ListExports(ctx, &dynamodb.ListExportsInput{TableArn: &p.options.TableArn})
	if err != nil {
		return fmt.Errorf("ListExports(): %w", err)
	}

	sem := semaphore.NewWeighted(p.options.Concurrency)
	ctx, cancel := p.options.withTimeout(ctx)
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)
	for _, summary := range out.ExportSummaries {
		if summary.ExportStatus != types.ExportStatusInProgress {
			continue
		}
		exportArn := summary.ExportArn
		var amount int64 = 1
		policy := &retry.Policy{
			MinDelay: p.options.InitialDelay,
			MaxDelay: p.options.MaxDelay,
			MaxCount: p.options.MaxAttempts,
		}
		if err := sem.Acquire(ctx, amount); err != nil {
			log.Error().Err(err).Str("exportArn", *exportArn).Msg("failed to acquire semaphore")
			return nil
		}
		eg.Go(func() error {
			defer sem.Release(amount)
			return policy.Do(ctx, func() error { return p.pollExport(ctx, *exportArn) })
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func (p *Poller) pollExport(ctx context.Context, exportArn string) error {
	l := log.With().Str("exportArn", exportArn).Logger()
	l.Debug().Msg("start describe export")
	out, err := p.client.DescribeExport(ctx, &dynamodb.DescribeExportInput{ExportArn: &exportArn})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if apiErr.ErrorFault() == smithy.FaultClient {
				return retry.MarkPermanent(err)
			}
		}
		return err
	}
	if out.ExportDescription.ExportStatus == types.ExportStatusInProgress {
		l.Debug().Msg("export is still in progress")
		return errExportNotFinite
	}
	l.Debug().Msg("export finishes")
	return nil
}
