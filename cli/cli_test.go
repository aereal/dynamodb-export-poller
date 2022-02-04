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
		{"no table ARN", []string{"me"}, statusNG},
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
