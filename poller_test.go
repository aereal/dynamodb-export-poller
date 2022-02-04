package ddbexportpoller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aereal/dynamodb-export-poller/internal/ddb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestPoller_PollExports(t *testing.T) {
	clear := setLoggerOutput(t)
	defer clear()

	testCases := []struct {
		name    string
		options PollerOptions
		onMock  func(mockClient *ddb.MockClient)
		want    error
	}{
		{
			"no exports",
			PollerOptions{
				TableArn:    "arn:aws:dynamodb:us-east-1:123456789012:table/my-table",
				Concurrency: 2,
				MaxAttempts: 1,
			},
			func(mockClient *ddb.MockClient) {
				listExports(mockClient, nil).Times(1)
			},
			nil,
		},
		{
			"no in-progress exports",
			PollerOptions{
				TableArn:    "arn:aws:dynamodb:us-east-1:123456789012:table/my-table",
				Concurrency: 2,
				MaxAttempts: 1,
			},
			func(mockClient *ddb.MockClient) {
				listExports(mockClient, []types.ExportSummary{
					{ExportArn: aws.String("arn:aws:dynamodb:us-east-1:123456789012:table/my-table/export/1234-5678"), ExportStatus: types.ExportStatusCompleted},
					{ExportArn: aws.String("arn:aws:dynamodb:us-east-1:123456789012:table/my-table/export/5678-1234"), ExportStatus: types.ExportStatusFailed},
				}).Times(1)
			},
			nil,
		},
		{
			"some in-progress exports found and it finishes until reached to retry limit",
			PollerOptions{
				TableArn:    "arn:aws:dynamodb:us-east-1:123456789012:table/my-table",
				Concurrency: 2,
				MaxAttempts: 2,
			},
			func(mockClient *ddb.MockClient) {
				listExports(
					mockClient,
					[]types.ExportSummary{{
						ExportArn:    aws.String("arn:aws:dynamodb:us-east-1:123456789012:table/my-table/export/9012-3456"),
						ExportStatus: types.ExportStatusInProgress}}).
					Times(1)
				seq(
					describeExport(
						mockClient,
						&types.ExportDescription{ExportStatus: types.ExportStatusInProgress}).
						Times(1),
					describeExport(
						mockClient,
						&types.ExportDescription{ExportStatus: types.ExportStatusCompleted}).
						Times(1),
				)
			},
			nil,
		},
		{
			"export jobs not finished in retry",
			PollerOptions{
				TableArn:    "arn:aws:dynamodb:us-east-1:123456789012:table/my-table",
				Concurrency: 2,
				MaxAttempts: 2,
			},
			func(mockClient *ddb.MockClient) {
				listExports(
					mockClient,
					[]types.ExportSummary{{
						ExportArn:    aws.String("arn:aws:dynamodb:us-east-1:123456789012:table/my-table/export/9012-3456"),
						ExportStatus: types.ExportStatusInProgress}}).
					Times(1)
				describeExport(
					mockClient,
					&types.ExportDescription{ExportStatus: types.ExportStatusInProgress}).
					Times(2)
			},
			errExportNotFinite,
		},
		{
			"client error",
			PollerOptions{
				TableArn:    "arn:aws:dynamodb:us-east-1:123456789012:table/my-table",
				Concurrency: 2,
				MaxAttempts: 2,
			},
			func(mockClient *ddb.MockClient) {
				listExports(
					mockClient,
					[]types.ExportSummary{{
						ExportArn:    aws.String("arn:aws:dynamodb:us-east-1:123456789012:table/my-table/export/9012-3456"),
						ExportStatus: types.ExportStatusInProgress}}).
					Times(1)
				mockClient.EXPECT().
					DescribeExport(gomock.Any(), gomock.Any()).
					Return(nil, &smithy.GenericAPIError{Code: "oops", Message: "oops", Fault: smithy.FaultClient}).
					Times(1)
			},
			&smithy.GenericAPIError{Code: "oops", Message: "oops", Fault: smithy.FaultClient},
		},
		{
			"server error",
			PollerOptions{
				TableArn:    "arn:aws:dynamodb:us-east-1:123456789012:table/my-table",
				Concurrency: 2,
				MaxAttempts: 2,
			},
			func(mockClient *ddb.MockClient) {
				listExports(
					mockClient,
					[]types.ExportSummary{{
						ExportArn:    aws.String("arn:aws:dynamodb:us-east-1:123456789012:table/my-table/export/9012-3456"),
						ExportStatus: types.ExportStatusInProgress}}).
					Times(1)
				mockClient.EXPECT().
					DescribeExport(gomock.Any(), gomock.Any()).
					Return(nil, &smithy.GenericAPIError{Code: "oops", Message: "oops", Fault: smithy.FaultServer}).
					Times(2)
			},
			&smithy.GenericAPIError{Code: "oops", Message: "oops", Fault: smithy.FaultServer},
		},
		{
			"other error",
			PollerOptions{
				TableArn:    "arn:aws:dynamodb:us-east-1:123456789012:table/my-table",
				Concurrency: 2,
				MaxAttempts: 2,
			},
			func(mockClient *ddb.MockClient) {
				listExports(
					mockClient,
					[]types.ExportSummary{{
						ExportArn:    aws.String("arn:aws:dynamodb:us-east-1:123456789012:table/my-table/export/9012-3456"),
						ExportStatus: types.ExportStatusInProgress}}).
					Times(1)
				mockClient.EXPECT().
					DescribeExport(gomock.Any(), gomock.Any()).
					Return(nil, errors.New("oops")).
					Times(2)
			},
			errors.New("oops"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			poller, err := NewPoller(tc.options)
			if err != nil {
				t.Fatalf("NewPoller(): %s", err)
			}
			mockClient := ddb.NewMockClient(ctrl)
			tc.onMock(mockClient)
			poller.client = mockClient

			ctx := context.Background()
			err = poller.PollExports(ctx)
			assertErr(t, err, tc.want)
		})
	}
}

func TestPollerOptions_validate(t *testing.T) {
	testCase := []struct {
		name    string
		options PollerOptions
		want    error
	}{
		{"ok", PollerOptions{TableArn: "arn:aws:dynamodb:us-east-1:123456789012:table/my-table", Concurrency: 1, MaxAttempts: 1}, nil},
		{"TableArn is empty", PollerOptions{Concurrency: 1, MaxAttempts: 1}, ErrTableArnRequired},
		{"malformed TableArn", PollerOptions{TableArn: "arn:aws:dynamodb:...", Concurrency: 1, MaxAttempts: 1}, ErrTableArnRequired},
		{"invalid concurrency", PollerOptions{TableArn: "arn:aws:dynamodb:us-east-1:123456789012:table/my-table", Concurrency: 0, MaxAttempts: 1}, ErrConcurrencyMustBePositive},
		{"zero maxAttmpts", PollerOptions{TableArn: "arn:aws:dynamodb:us-east-1:123456789012:table/my-table", Concurrency: 1, MaxAttempts: 0}, nil},
	}
	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.options.validate()
			assertErr(t, got, tc.want)
		})
	}
}

func assertErr(t *testing.T, got, want error) {
	t.Helper()
	if wantErr(got) != wantErr(want) {
		t.Errorf("error existence:\n\twant=%v\n\tgot=%v", wantErr(want), wantErr(got))
	}
	if errMsg(got) != errMsg(want) {
		t.Errorf("error message mismatch:\n\twant=%s\n\tgot=%s", errMsg(want), errMsg(got))
	}
}

func wantErr(err error) bool {
	return err != nil
}

func errMsg(err error) string {
	if err == nil {
		return ""
	}
	var merr *multierror.Error
	if errors.As(err, &merr) {
		merr.ErrorFormat = func(errs []error) string {
			xs := make([]string, len(errs))
			for i, err := range errs {
				xs[i] = err.Error()
			}
			return strings.Join(xs, "\n")
		}
		return merr.Error()
	}
	return err.Error()
}

func setLoggerOutput(t *testing.T) func() {
	t.Helper()
	orig := log.Logger
	log.Logger = log.Output(zerolog.NewTestWriter(t))
	return func() {
		log.Logger = orig
	}
}

func listExports(mockClient *ddb.MockClient, summaries []types.ExportSummary) *gomock.Call {
	return mockClient.EXPECT().
		ListExports(gomock.Any(), gomock.Any()).
		Return(&dynamodb.ListExportsOutput{ExportSummaries: summaries}, nil)
}

func describeExport(mockClient *ddb.MockClient, description *types.ExportDescription) *gomock.Call {
	return mockClient.EXPECT().
		DescribeExport(gomock.Any(), gomock.Any()).
		Return(&dynamodb.DescribeExportOutput{ExportDescription: description}, nil)
}

func seq(calls ...*gomock.Call) *gomock.Call {
	if len(calls) == 0 {
		panic(errors.New("calls must be given"))
	}
	var curr *gomock.Call
	for i := len(calls) - 1; i >= 0; i-- {
		if curr == nil {
			curr = calls[i]
			continue
		}
		curr.After(calls[i])
		curr = calls[i]
	}
	return curr
}
