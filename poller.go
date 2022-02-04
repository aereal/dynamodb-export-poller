package ddbexportpoller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aereal/dynamodb-export-poller/internal/ddb"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog/log"
	"github.com/shogo82148/go-retry"
	"golang.org/x/sync/semaphore"
)

var (
	// ErrTableArnRequired is an error that means mandatory table ARN is not passed
	ErrTableArnRequired = errors.New("table ARN required")

	// ErrConcurrencyMustBePositive is an error that means given concurrency is too small
	ErrConcurrencyMustBePositive = errors.New("concurrency must greater than 0")

	// ErrExportHasNotBeenFinished is an error that ongoing export jobs have not been finished until the deadline.
	//
	// The error is not returned if MaxAttempts and Timeout are zero.
	ErrExportHasNotBeenFinished = errors.New("export has not been finished")
)

// PollerOptions is a set of Poller's options
type PollerOptions struct {
	// InitialDelay is used for first interval
	InitialDelay time.Duration

	// MaxDelay is maximum interval for each requests
	MaxDelay time.Duration

	// MaxAttempts is a number to send export job status check requests
	MaxAttempts int

	// Concurrency means max number of requests at the same time
	Concurrency int64

	// Timeout is used for all export job status check requests. No requests are sent over this timeout.
	Timeout time.Duration
}

func (o PollerOptions) validate() error {
	var err error
	if o.Concurrency <= 0 {
		err = multierror.Append(err, ErrConcurrencyMustBePositive)
	}
	return err
}

var noop = func() {}

func (o PollerOptions) withTimeout(parent context.Context) (context.Context, func()) {
	if o.Timeout == 0 {
		return parent, noop
	}
	return context.WithTimeout(parent, o.Timeout)
}

// NewPoller creates new Poller.
//
// An error may be returned if you passed invalid options.
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
	client  ddb.Client
}

const semaphoreWorkerAmount int64 = 1

// PollExportsOnTable polls ongoing export job status changes.
//
// You can configure polling behaviors through PollerOptions.
func (p *Poller) PollExportsOnTable(ctx context.Context, tableArn string) error {
	if !arn.IsARN(tableArn) {
		return ErrTableArnRequired
	}

	out, err := p.client.ListExports(ctx, &dynamodb.ListExportsInput{TableArn: &tableArn})
	if err != nil {
		return fmt.Errorf("ListExports(): %w", err)
	}

	sem := semaphore.NewWeighted(p.options.Concurrency)
	ctx, cancel := p.options.withTimeout(ctx)
	defer cancel()
	meg := &multierror.Group{}
	for _, summary := range out.ExportSummaries {
		if summary.ExportStatus != types.ExportStatusInProgress {
			continue
		}
		exportArn := summary.ExportArn
		if err := sem.Acquire(ctx, semaphoreWorkerAmount); err != nil {
			log.Error().Err(err).Str("exportArn", *exportArn).Msg("failed to acquire semaphore")
			return nil
		}
		meg.Go(func() error {
			defer sem.Release(semaphoreWorkerAmount)
			return p.pollExportWithRetries(ctx, *exportArn)
		})
	}
	if err := meg.Wait().ErrorOrNil(); err != nil {
		return err
	}

	return nil
}

func (p *Poller) pollExportWithRetries(ctx context.Context, exportArn string) error {
	policy := &retry.Policy{
		MinDelay: p.options.InitialDelay,
		MaxDelay: p.options.MaxDelay,
		MaxCount: p.options.MaxAttempts,
	}
	return policy.Do(ctx, func() error { return p.pollExport(ctx, exportArn) })
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
		return ErrExportHasNotBeenFinished
	}
	l.Debug().Msg("export finishes")
	return nil
}
