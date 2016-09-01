package influxdb

import "io"

// Series represents a series included within the result.
type Series struct {
	Name    string
	Tags    map[string]string
	Columns []string
	Values  [][]interface{}
}

// SameSeries returns if this is the same series as the other series.
func (s *Series) SameSeries(o *Series) bool {
	if s.Name != o.Name || len(s.Tags) != len(o.Tags) {
		return false
	}

	for k, v1 := range s.Tags {
		v2, ok := o.Tags[k]
		if !ok || v1 != v2 {
			return false
		}
	}
	return true
}

// Message is a user-facing message for informational messages sent by the
// server for a Result.
type Message struct {
	Level string
	Text  string
}

// Result is a single result to be read from the query body.
type Result struct {
	Series   []Series
	Messages []Message
}

// ResultError encapsulates an error from a result.
type ResultError struct {
	Err string
}

// Error returns the error returned as part of the result.
func (e *ResultError) Error() string {
	return e.Err
}

// Reader reads query results from a stream.
type Reader interface {
	// Read reads the next Result into the Result struct passed in.
	// This will allocate memory if the slices inside of the Result struct
	// are not large enough to accomodate the results.
	// If nil is passed to this method, the results are discarded and only
	// an error is returned if one happened.
	// If the next result contains an error, it will be encapsulated within a
	// ResultError. Any other errors will be returned as-is including io.EOF.
	Read(*Result) error
}

// ReadCloser combines the Reader and io.Closer interfaces.
type ReadCloser interface {
	Reader
	io.Closer
}

// NewReader constructs a new reader from the io.Reader and parses it with
// the formatter.
// The following formatters are supported:
//   * json, application/json
//   * csv, text/csv
func NewReader(r io.Reader, format string) (ReadCloser, error) {
	return (*nilReader)(nil), nil
}

type nilReader struct{}

func (r *nilReader) Read(*Result) error { return nil }
func (r *nilReader) Close() error       { return nil }

// ForEach reads every result from the reader and executes the function for
// each one. When an error is returned, the results will stop being processed
// and the error will be returned.
func ForEach(r Reader, fn func(*Result) error) error {
	return nil
}
