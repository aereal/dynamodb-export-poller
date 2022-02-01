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

func NewPoller(tableArn string, initialDelay, maxDelay time.Duration, maxWorkers int64, maxAttempts int) (*Poller, error) {
	if tableArn == "" {
		return nil, ErrTableArnRequired
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("LoadDefaultConfig(): %w", err)
	}

	poller := &Poller{
		tableArn:     tableArn,
		initialDelay: initialDelay,
		maxDelay:     maxDelay,
		maxWorkers:   maxWorkers,
		maxAttempts:  maxAttempts,
	}
	poller.client = dynamodb.NewFromConfig(cfg)
	return poller, nil
}

type Poller struct {
	tableArn     string
	initialDelay time.Duration
	maxDelay     time.Duration
	maxAttempts  int
	maxWorkers   int64

	client *dynamodb.Client
}

func (p *Poller) PollExports(ctx context.Context) error {
	out, err := p.client.ListExports(ctx, &dynamodb.ListExportsInput{TableArn: &p.tableArn})
	if err != nil {
		return fmt.Errorf("ListExports(): %w", err)
	}

	sem := semaphore.NewWeighted(p.maxWorkers)
	eg, ctx := errgroup.WithContext(ctx)
	for _, summary := range out.ExportSummaries {
		if summary.ExportStatus != types.ExportStatusInProgress {
			continue
		}
		exportArn := summary.ExportArn
		var amount int64 = 1
		policy := &retry.Policy{
			MinDelay: p.initialDelay,
			MaxDelay: p.maxDelay,
			MaxCount: p.maxAttempts,
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
