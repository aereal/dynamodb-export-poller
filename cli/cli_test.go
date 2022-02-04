package cli

import (
	"bytes"
	"testing"
)

func TestCLI(t *testing.T) {
	testCases := []struct {
		name       string
		argv       []string
		wantStatus int
	}{
		{"neither tableArn or exportArn specified", []string{"me"}, statusNG},
		{"both tableArn and exportArn specified", []string{"me", "-table-arn", "arn:aws:dynamodb:us-east-1:123456789012:table/my-table", "-export-arn", "arn:aws:dynamodb:us-east-1:123456789012:table/my-table/export/9012-3456"}, statusNG},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stream := new(bytes.Buffer)
			app := NewApp(stream)
			gotStatus := app.Run(tc.argv)
			if gotStatus != tc.wantStatus {
				t.Errorf("status:\n\twant=%d\n\tgot=%d", tc.wantStatus, gotStatus)
			}
			t.Log(stream.String())
		})
	}
}
