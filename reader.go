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

// Reader reads query results from a stream.
type Reader interface {
	// Read reads the next Result into the Result struct passed in.
	// This will allocate memory if the slices inside of the Result struct
	// are not large enough to accomodate the results.
	Read(*Result) error
}

// ReadCloser combines the Reader and io.Closer interfaces.
type ReadCloser interface {
	Reader
	io.Closer
}

// NewReader constructs a new reader from the io.Reader and the QueryMeta data.
func NewReader(r io.Reader, meta QueryMeta) (Reader, error) {
	return nil, nil
}

// ForEach reads every result from the reader and executes the function for
// each one. When an error is returned, the results will stop being processed
// and the error will be returned.
func ForEach(r Reader, fn func(*Result) error) error {
	return nil
}
