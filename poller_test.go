package ddbexportpoller

import (
	"errors"
	"testing"
)

func TestPollerOptions_validate(t *testing.T) {
	testCase := []struct {
		name    string
		options PollerOptions
		want    error
	}{
		{"ok", PollerOptions{TableArn: "arn:aws:dynamodb:us-east-1:123456789012:table/my-table"}, nil},
		{"TableArn is empty", PollerOptions{}, ErrTableArnRequired},
		{"malformed TableArn", PollerOptions{TableArn: "arn:aws:dynamodb:..."}, ErrTableArnRequired},
	}
	for _, tc := range testCase {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.options.validate()
			if wantErr(tc.want) != wantErr(got) {
				t.Errorf("error existence:\n\twant=%v\n\tgot=%v", wantErr(tc.want), wantErr(got))
			}
			if !errors.Is(got, tc.want) {
				t.Errorf("error type mismatch:\n\twant=%T\n\tgot=%T", tc.want, got)
			}
			if errMsg(got) != errMsg(tc.want) {
				t.Errorf("error message mismatch:\n\twant=%s\n\tgot=%s", errMsg(tc.want), errMsg(got))
			}
		})
	}
}

func wantErr(err error) bool {
	return err != nil
}

func errMsg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
