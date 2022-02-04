package ddbexportpoller

import (
	"errors"
	"strings"
	"testing"

	"github.com/hashicorp/go-multierror"
)

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
		{"invalid maxAttmpts", PollerOptions{TableArn: "arn:aws:dynamodb:us-east-1:123456789012:table/my-table", Concurrency: 1, MaxAttempts: 0}, ErrInfiniteRetries},
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
	if !errors.Is(got, want) {
		t.Errorf("error type mismatch:\n\twant=%T\n\tgot=%T", want, got)
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
