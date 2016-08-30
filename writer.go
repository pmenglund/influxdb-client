package influxdb

import "io"

// Writer is a generic interface for writing a batch of points to InfluxDB.
type Writer interface {
	Write(...Point) error
}

// WriteCloser combines the Writer and io.Closer interfaces.
type WriteCloser interface {
	Writer
	io.Closer
}

// WriteError is an error returned by the server to indicate a permanent error
// with the written points.
type WriteError struct {
}

func (e WriteError) Error() string {
	return ""
}
