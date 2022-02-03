//go:generate go run github.com/golang/mock/mockgen -package ddb -destination ./mock.go github.com/aereal/dynamodb-export-poller/internal/ddb Client

package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Client interface {
	DescribeExport(ctx context.Context, params *dynamodb.DescribeExportInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeExportOutput, error)
	ListExports(ctx context.Context, params *dynamodb.ListExportsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListExportsOutput, error)
}
