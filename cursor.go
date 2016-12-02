package influxdb

import (
	"io"
	"time"
)

// Cursor is a cursor that reads and decodes a ResultSet.
type Cursor interface {
	// NextSet will return the next ResultSet. This invalidates the previous
	// ResultSet returned by this Cursor and discards any remaining data to be
	// read (including any remaining partial results that need to be read).
	// Depending on the implementation of the cursor, previous ResultSet's may
	// still return results even after being invalidated.
	NextSet() (ResultSet, error)

	// Close closes the cursor so the underlying stream will be closed if one exists.
	Close() error
}

// ResultSet encapsulates a result from a single command.
type ResultSet interface {
	// Columns returns the column names for this ResultSet.
	Columns() []string

	// Index returns the array index for the column name. If a column with that
	// name does not exist, this returns -1.
	Index(name string) int

	// Messages returns the informational messages sent by the server for this ResultSet.
	Messages() []*Message

	// NextSeries returns the next series in the result.
	NextSeries() (Series, error)
}

// Series encapsulates a series within a ResultSet.
type Series interface {
	// Name returns the measurement name associated with this series.
	Name() string

	// Tags returns the tags for this series. They are in sorted order.
	Tags() Tags

	// Columns returns the column names associated with this Series.
	Columns() []string

	// Len returns currently known length of the series. The length returned is
	// the cumulative length of the entire series, not just the current batch.
	// If the entire series hasn't been read because it is being sent in
	// partial chunks, this returns false for complete.
	Len() (n int, complete bool)

	// NextRow returns the next row in the result.
	NextRow() (Row, error)
}

// Row is a row of values in the ResultSet.
type Row interface {
	// Time returns the time column as a time.Time if it exists in the Row.
	Time() time.Time

	// Value returns value at index. If an invalid index is given, this will panic.
	Value(index int) interface{}

	// Values returns the values from the row as an array slice.
	Values() []interface{}

	// ValueByName returns the value by a named column. If the column does not
	// exist, this will return nil.
	ValueByName(column string) interface{}
}

// NewCursor constructs a new cursor from the io.ReadCloser and parses it with
// the appropriate decoder for the format. The following formatters are supported:
// json (application/json)
func NewCursor(r io.ReadCloser, format string) (Cursor, error) {
	switch format {
	case "json", "application/json":
		return newJSONCursor(r), nil
	default:
		return nil, ErrUnknownFormat{Format: format}
	}
}

// Message is an informational message from the server.
type Message struct {
	Level string `json:"level"`
	Text  string `json:"text"`
}

func (m *Message) String() string {
	return m.Text
}
