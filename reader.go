package influxdb

/*
// Series represents a series included within the result.
type Series struct {
	Name    string            `json:"name"`
	Tags    map[string]string `json:"tags"`
	Columns []string          `json:"columns"`
	Values  [][]interface{}   `json:"values"`
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
	Level string `json:"level"`
	Text  string `json:"text"`
}

// Result is a single result to be read from the query body.
type Result struct {
	Series   []Series  `json:"series"`
	Messages []Message `json:"messages"`
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
	switch format {
	case "json", "application/json":
		return &jsonReader{
			Reader:  r,
			Decoder: json.NewDecoder(r),
		}, nil
	}
	return (*nilReader)(nil), nil
}

type jsonReader struct {
	Reader  io.Reader
	Decoder *json.Decoder
}

func (r *jsonReader) Close() error {
	if r, ok := r.Reader.(io.Closer); ok {
		return r.Close()
	}
	return nil
}

func (r *jsonReader) Read(result *Result) error {
	if result != nil {
		var v struct {
			Result
			Error string `json:"error"`
		}

		if err := r.Decoder.Decode(&v); err != nil {
			return err
		} else if v.Error != "" {
			return &ResultError{Err: v.Error}
		}
		result.Series = v.Result.Series
		result.Messages = v.Result.Messages
		return nil
	}

	var v struct {
		Error string `json:"error"`
	}

	if err := r.Decoder.Decode(&v); err != nil {
		return err
	} else if v.Error != "" {
		return &ResultError{Err: v.Error}
	}
	return nil
}

type nilReader struct{}

func (r *nilReader) Read(*Result) error { return io.EOF }
func (r *nilReader) Close() error       { return nil }

// ForEach reads every result from the reader and executes the function for
// each one. When an error is returned, the results will stop being processed
// and the error will be returned.
func ForEach(r Reader, fn func(*Result) error) error {
	return nil
}
*/
