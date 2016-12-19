package influxdb

import (
	"encoding/json"
	"io"
	"sort"
	"time"
)

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

func (c *jsonCursor) Hijack() (io.ReadCloser, error) {
	return c.r, nil
}

type jsonResult struct {
	Series []struct {
		Name    string            `json:"name"`
		Tags    map[string]string `json:"tags"`
		Columns []string          `json:"columns"`
		Values  [][]interface{}   `json:"values"`
		Partial bool              `json:"partial"`
	} `json:"series"`
	MessageList []*Message `json:"messages"`
	Partial     bool       `json:"partial"`
	Err         string     `json:"error"`

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

func (r *jsonResult) Messages() []*Message {
	return r.MessageList
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

		// Mark the current series is invalid to prevent it from trying to
		// modify the ResultSet. We don't remove the reference to the
		// ResultSet so it can still access the immutable data members, but not
		// modify mutable sections.
		r.series.invalid = true

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
	invalid bool
}

func (s *jsonSeries) Name() string {
	return s.name
}

func (s *jsonSeries) Tags() Tags {
	return s.tags
}

func (s *jsonSeries) Columns() []string {
	return s.r.Columns()
}

func (s *jsonSeries) Len() (n int, complete bool) {
	return s.sz, !s.partial
}

func (s *jsonSeries) NextRow() (Row, error) {
	for len(s.values) == 0 {
		if !s.partial {
			return nil, io.EOF
		} else if s.invalid {
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

	// Attempt to cast this to a string or float64. The time column can be
	// either one of those two values. It will either be the number of
	// nanoseconds since the epoch or a string in RFC3339Nano format.
	switch v := v.(type) {
	case string:
		// Parse the time using RFC3339Nano. This also accepts RFC3339 without
		// nanoseconds. If it doesn't parse, then the time column does not contain
		// a time value.
		t, _ := time.Parse(time.RFC3339Nano, v)
		return t
	case float64:
		return time.Unix(0, int64(v)).UTC()
	}
	return time.Time{}
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
