//go:build go1.16
// +build go1.16

package cli

import "io"

func init() {
	defaultWriter = io.Discard
}
