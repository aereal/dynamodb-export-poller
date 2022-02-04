//go:build !go1.16
// +build !go1.16

package cli

// implementations almost taken from https://pkg.go.dev/io#Discard
// but discardWriterCompat does not implement io.ReadFrom.
type discardWriterCompat int

func (discardWriterCompat) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func init() {
	defaultWriter = discardWriterCompat(0)
}
