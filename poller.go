package ddbexportpoller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/rs/zerolog/log"
	"github.com/shogo82148/go-retry"
	"golang.org/x/sync/errgroup"
)

var (
	ErrTableArnRequired = errors.New("table ARN required")

	errExportNotFinite = errors.New("export is not finite")
)

func NewPoller(tableArn string, initialDelay time.Duration) (*Poller, error) {
	if tableArn == "" {
		return nil, ErrTableArnRequired
	}
	return &Poller{
		tableArn:     tableArn,
		initialDelay: initialDelay,
	}, nil
}

type Poller struct {
	tableArn     string
	initialDelay time.Duration
}

func (p *Poller) PollExports(ctx context.Context) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("LoadDefaultConfig(): %w", err)
	}
	client := dynamodb.NewFromConfig(cfg)

	out, err := client.ListExports(ctx, &dynamodb.ListExportsInput{TableArn: &p.tableArn})
	if err != nil {
		return fmt.Errorf("ListExports(): %w", err)
	}

	eg, ctx := errgroup.WithContext(ctx)
	for _, summary := range out.ExportSummaries {
		if summary.ExportStatus != types.ExportStatusInProgress {
			continue
		}
		exportArn := summary.ExportArn
		f := func() error {
			l := log.With().Str("exportArn", *exportArn).Logger()
			l.Debug().Msg("start describe export")
			out, err := client.DescribeExport(ctx, &dynamodb.DescribeExportInput{ExportArn: exportArn})
			if err != nil {
				// TODO: check ErrorFault markPermanent
				return err
			}
			if out.ExportDescription.ExportStatus == types.ExportStatusInProgress {
				l.Debug().Msg("export is still in progress")
				return errExportNotFinite
			}
			l.Debug().Msg("export finishes")
			return nil
		}
		policy := &retry.Policy{MinDelay: p.initialDelay}
		eg.Go(func() error {
			// TODO: use semaphore to control concurrency
			return policy.Do(ctx, f)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}
