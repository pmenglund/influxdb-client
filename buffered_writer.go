package influxdb

import (
	"bytes"
	"time"
)

// BufferOptions contains options for configuring how often the BufferedWriter
// will flush metrics and how large the buffer will be.
type BufferOptions struct {
	// BufferSize is the maximum buffer size before points will be
	// automatically flushed.
	BufferSize int

	// FlushInterval is the time interval to perform a Flush operation. A flush
	// that fails when this interval is reached will call OnFlushError.
	FlushInterval time.Duration

	// RetryLimit is the number of maximum number of retries before it is
	// considered a failure. If the Writer returns a WriteError, no retry will
	// be attempted since the failure was with the data and not the connection.
	RetryLimit int

	// OnFlushError will be called if a batch of points fails to write to the
	// underlying Writer during a flush. If OnFlushError is unset, the error
	// will be returned on a Write or Flush. If OnFlushError returns an error,
	// this error will be returned on a Write or Flush. The OnFlushError
	// function gets called in the same goroutine as the call that initiated
	// it. This will only be called if the RetryLimit is exceeeded. Do not try
	// to rewrite the points in this function as a way of retrying a failed
	// write.
	OnFlushError func(err error) error
}

// BufferedWriter buffers points and writes them to the underlying Writer
// either after the buffer has been filled or the FlushInterval has been
// reached.
type BufferedWriter struct {
	w   Writer
	opt BufferOptions

	buf bytes.Buffer
	n   int
}

// NewBufferedWriter creates a new BufferedWriter.
func NewBufferedWriter(w Writer, opt *BufferOptions) *BufferedWriter {
	return nil
}

// Write writes the points to buffer. If the buffer exceeds the BufferSize, the
// buffer will be flushed. This method does not return an error from a failed
// flush and will not wait for the flush to complete. Use OnFlushError to act
// on any errors from automatic flushes.
func (b *BufferedWriter) WritePoint(points ...Point) error {
	return nil
}

// Close closes the BufferedWriter. It will Flush any remaining data. Any
// errors from Flush will be returned here.
func (b *BufferedWriter) Close() error {
	return nil
}

// Flush will force the current buffer to flush and write any buffered metrics
// to the Writer. If there was some error while writing, OnFlushError will be
// called if it is set and any error returned from that will be returned
// instead.
func (b *BufferedWriter) Flush() error {
	var err error

	if err != nil && b.OnFlushError != nil {
		err = b.OnFlushError(err)
	}
	return err
}
