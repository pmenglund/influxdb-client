package influxdb

import (
	"encoding/json"
	"io"
	"sort"
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

	//ResultSet
}

// ResultSet encapsulates a result from a single command.
type ResultSet interface {
	// Columns returns the column names for this ResultSet.
	Columns() []string

	// Index returns the array index for the column name. If a column with that
	// name does not exist, this returns -1.
	Index(name string) int

	// NextSeries returns the next series in the result.
	NextSeries() (Series, error)
}

// Series encapsulates a series within a ResultSet.
type Series interface {
	// Name returns the measurement name associated with this series.
	Name() string

	// Tags returns the tags for this series. They are in sorted order.
	Tags() Tags

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
//   * json, application/json
func NewCursor(r io.ReadCloser, format string) (Cursor, error) {
	switch format {
	case "json", "application/json":
		return newJSONCursor(r), nil
	default:
		return nil, ErrUnknownFormat{Format: format}
	}
}

type jsonCursor struct {
	r   io.ReadCloser
	dec *json.Decoder

	cur *jsonResult
	buf struct {
		Results []*jsonResult `json:"results"`
	}
}

func newJSONCursor(r io.ReadCloser) *jsonCursor {
	return &jsonCursor{
		r:   r,
		dec: json.NewDecoder(r),
	}
}

func (c *jsonCursor) NextSet() (ResultSet, error) {
	if c.cur != nil {
		// Mark the current result in a way so that, if it is read in the
		// future, it will not try to modify the cursor. It is now considered
		// invalid and only stale data will be readable.
		c.cur.cur = nil

		// The current result is partial. Skip results until we get one that is
		// not marked partial.
		for c.cur.Partial {
			for len(c.buf.Results) == 0 {
				if err := c.dec.Decode(&c.buf); err != nil {
					if err == io.EOF {
						err = io.ErrUnexpectedEOF
					}
					return nil, err
				}
			}

			// Move to the next Result.
			c.cur = c.buf.Results[0]
			c.buf.Results = c.buf.Results[1:]
		}

		// Mark the final result of this set as nil so we do not use it.
		c.cur = nil
	}

	// Fill the results buffer with results until we have something.
	for len(c.buf.Results) == 0 {
		if err := c.dec.Decode(&c.buf); err != nil {
			return nil, err
		}
	}

	// Keep track of the currently active ResultSet so we can later invalidate
	// it if we need to.
	c.cur = c.buf.Results[0]
	if c.cur.Err != "" {
		// Return an error instead of the ResultSet if the result contained an error.
		return nil, ErrResult{Err: c.cur.Err}
	}

	c.buf.Results = c.buf.Results[1:]
	if c.cur.Partial {
		c.cur.cur = c
	}

	// Copy the columns from the result to an immutable variable so that we
	// don't have to worry about modifications to the column names.
	if len(c.cur.Series) > 0 {
		c.cur.columns = c.cur.Series[0].Columns
	}
	return c.cur, nil
}

func (c *jsonCursor) Close() error {
	if err := c.r.Close(); err != nil {
		return err
	}
	c.buf.Results = nil
	return nil
}

type jsonResult struct {
	Series []struct {
		Name    string            `json:"name"`
		Tags    map[string]string `json:"tags"`
		Columns []string          `json:"columns"`
		Values  [][]interface{}   `json:"values"`
		Partial bool              `json:"partial"`
	} `json:"series"`
	Partial bool   `json:"partial"`
	Err     string `json:"error"`

	index         int
	columns       []string
	columnsByName map[string]int
	cur           *jsonCursor
	series        *jsonSeries
}

// Columns returns the columns for this result.
//
// Columns is just a gigantic mistake in the JSON output for InfluxDB. Columns
// don't change for the entire result set, but for whatever reason, column
// information is attached to the series instead of to the ResultSet. So
// instead of sanely just retrieving the columns once from the ResultSet, we do
// this semi-stupid thing of retrieving the columns from the first series in
// the array.
func (r *jsonResult) Columns() []string {
	return r.columns
}

func (r *jsonResult) Index(name string) int {
	if len(r.columns) == 0 {
		return -1
	}

	// If we do not have a lookup index, retrieve the columns and preallocate a
	// map for the index.
	if r.columnsByName == nil {
		r.columnsByName = make(map[string]int, len(r.columns))
	} else if i, ok := r.columnsByName[name]; ok {
		// Otherwise check that index to see if we have the name we're looking for.
		return i
	}

	// Iterate through the columns until we find the column we are looking for.
	// Cache all of the results in the map as we check to make future lookups
	// faster.
	for i := len(r.columnsByName); i < len(r.columns); i++ {
		s := r.columns[i]
		r.columnsByName[s] = i
		if name == s {
			return i
		}
	}

	// Column not found.
	return -1
}

func (r *jsonResult) NextSeries() (Series, error) {
	// If we have a current series, mark it as invalid. If that series was
	// partial, we need to advance this ResultSet so it skips past the next
	// series. The most difficult part of this is that if a series is partial,
	// that means the result itself is also probably partial so we need to
	// handle both of these.
	if r.series != nil {
		// If this series is partial, we need to advance this ResultSet so it
		// skips past the next series. The most difficult part of this is that
		// if a series is partial, that means the result itself is also
		// probably partial so we need to handle both of these.
		if r.series.partial {
			for {
				// If the next series is not within the current ResultSet, read
				// the next one if the ResultSet was marked as partial.
				for r.index >= len(r.Series) {
					// If the result is not partial, then we have had our output truncated.
					if !r.Partial {
						return nil, ErrSeriesTruncated
					} else if r.cur == nil {
						return nil, io.ErrUnexpectedEOF
					}

					// Fill the results buffer with results until we have something.
					for len(r.cur.buf.Results) == 0 {
						if err := r.cur.dec.Decode(&r.cur.buf); err != nil {
							if err == io.EOF {
								err = io.ErrUnexpectedEOF
							}
							return nil, err
						}
					}

					// Copy the state of the next result into the current
					// result. We must use the same result struct so that we
					// keep all references.
					result := r.cur.buf.Results[0]
					if result.Err != "" {
						return nil, ErrResult{Err: result.Err}
					}
					r.cur.buf.Results = r.cur.buf.Results[1:]
					r.Series = result.Series
					r.Partial = result.Partial
					r.index = 0
				}

				if !r.Series[r.index].Partial {
					break
				}
				r.index++
			}

			// Skip past the next series because the previous series was partial.
			r.index++
		}

		// Remove the ResultSet from the active series since it is no longer valid.
		// This leaves the series as stale, but technically still usable with cached data.
		r.series.r = nil

		// Remove the reference to the current set.
		r.series = nil
	}

	for r.index >= len(r.Series) {
		// If the result is not partial, then there are no more series in this ResultSet.
		if !r.Partial {
			return nil, io.EOF
		} else if r.cur == nil {
			// If we have been detached from the main cursor, we reach an unexpected EOF.
			return nil, io.ErrUnexpectedEOF
		}

		// Fill the results buffer with results until we have something.
		for len(r.cur.buf.Results) == 0 {
			if err := r.cur.dec.Decode(&r.cur.buf); err != nil {
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
				return nil, err
			}
		}

		// Copy the state of the next result into the current
		// result. We must use the same result struct so that we
		// keep all references.
		result := r.cur.buf.Results[0]
		if result.Err != "" {
			return nil, ErrResult{Err: result.Err}
		}
		r.cur.buf.Results = r.cur.buf.Results[1:]
		r.Series = result.Series
		r.Partial = result.Partial
		r.index = 0
	}

	// Retrieve the index of the next series and initialize the series.
	v := r.Series[r.index]
	r.index++

	// Retrieve and sort the tags.
	var tags Tags
	if len(v.Tags) > 0 {
		tags = make(Tags, 0, len(v.Tags))
		for k, v := range v.Tags {
			tags = append(tags, Tag{Key: k, Value: v})
		}
		sort.Sort(tags)
	}

	r.series = &jsonSeries{
		name:    v.Name,
		tags:    tags,
		sz:      len(v.Values),
		r:       r,
		values:  v.Values,
		partial: v.Partial,
	}
	return r.series, nil
}

type jsonSeries struct {
	name string
	tags Tags
	sz   int

	r       *jsonResult
	values  [][]interface{}
	partial bool
}

func (s *jsonSeries) Name() string {
	return s.name
}

func (s *jsonSeries) Tags() Tags {
	return s.tags
}

func (s *jsonSeries) Len() (n int, complete bool) {
	return s.sz, !s.partial
}

func (s *jsonSeries) NextRow() (Row, error) {
	for len(s.values) == 0 {
		if !s.partial {
			return nil, io.EOF
		} else if s.r == nil {
			return nil, io.ErrUnexpectedEOF
		}

		// As this is a partial series, look for the next series to continue
		// finding values.
		for s.r.index >= len(s.r.Series) {
			// If the result is not partial, then we have had our output truncated.
			if !s.r.Partial {
				return nil, ErrSeriesTruncated
			} else if s.r.cur == nil {
				return nil, io.ErrUnexpectedEOF
			}

			// Fill the results buffer with results until we have something.
			if err := s.r.cur.dec.Decode(&s.r.cur.buf); err != nil {
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
				return nil, err
			}

			// Copy the state of the next result into the current
			// result. We must use the same result struct so that we
			// keep all references.
			result := s.r.cur.buf.Results[0]
			if result.Err != "" {
				return nil, ErrResult{Err: result.Err}
			}
			s.r.cur.buf.Results = s.r.cur.buf.Results[1:]
			s.r.Series = result.Series
			s.r.Partial = result.Partial
			s.r.index = 0
		}

		v := s.r.Series[s.r.index]
		s.r.index++

		s.values = v.Values
		s.sz += len(v.Values)
		s.partial = v.Partial
	}

	v := s.values[0]
	s.values = s.values[1:]
	return jsonRow{values: v, result: s.r}, nil
}

type jsonRow struct {
	values []interface{}
	result *jsonResult
}

func (r jsonRow) Time() time.Time {
	// Retrieve the value for the time column if it exists. This is usually the
	// first column so this should be pretty fast. Column indexing is also
	// shared between rows.
	v := r.ValueByName("time")
	if v == nil {
		return time.Time{}
	}

	// Attempt to cast this to a string. We return string values for the time.
	// If we don't get a string, something weird has happened so we don't have
	// a time.
	ts, ok := v.(string)
	if !ok {
		return time.Time{}
	}

	// Parse the time using RFC3339Nano. This also accepts RFC3339 without
	// nanoseconds. If it doesn't parse, then the time column does not contain
	// a time value.
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}
	}
	return t
}

func (r jsonRow) Value(index int) interface{} {
	return r.values[index]
}

func (r jsonRow) Values() []interface{} {
	return r.values
}

func (r jsonRow) ValueByName(column string) interface{} {
	index := r.result.Index(column)
	if index == -1 {
		return nil
	}
	return r.values[index]
}

type nilResultSet struct{}

func (*nilResultSet) Columns() []string           { return nil }
func (*nilResultSet) Index(string) int            { return -1 }
func (*nilResultSet) NextSeries() (Series, error) { return nil, io.EOF }
