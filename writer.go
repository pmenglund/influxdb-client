package influxdb

import "io"

// Writer is a generic interface for writing a batch of points to InfluxDB.
type Writer interface {
	// WritePoint encodes and writes points to the underlying writer.
	WritePoint(...Point) error
}

// writer wraps an io.Writer to encode points with an Encoder.
type writer struct {
	io.Writer
	p Protocol
}

// NewPointWriter creates a new PointWriter with the io.Writer and Protocol.
func NewWriter(w io.Writer, p Protocol) Writer {
	return &writer{
		Writer: w,
		p:      p,
	}
}

func (w *writer) WritePoint(pts ...Point) error {
	for _, pt := range pts {
		if err := w.p.Encode(w.Writer, &pt); err != nil {
			return err
		}
	}
	return nil
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
