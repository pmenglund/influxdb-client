package influxdb

// QueryOptions is a set of configuration options for configuring queries.
type QueryOptions struct {
	Database  string
	Chunked   bool
	ChunkSize int
	Pretty    bool
	Format    string
	Async     bool
	Params    map[string]interface{}
}

// Clone creates a copy of the QueryOptions.
func (opt *QueryOptions) Clone() QueryOptions {
	clone := *opt
	clone.Params = make(map[string]interface{})
	for k, v := range opt.Params {
		clone.Params[k] = v
	}
	return clone
}

// Querier holds onto query options and acts as a convenience method for performing queries.
type Querier struct {
	c *Client
	QueryOptions
}

// Select executes a query with GET and returns a Cursor that will parse the
// results from the stream. Use Execute for any queries that modify the database.
func (q *Querier) Select(query interface{}, opts ...QueryOption) (Cursor, error) {
	opt := q.QueryOptions
	if len(opts) > 0 {
		opt = opt.Clone()
		for _, f := range opts {
			f.apply(&opt)
		}
	}

	req, err := q.c.NewReadonlyQueryRequest(query, opt)
	if err != nil {
		return nil, err
	}

	resp, err := q.c.Client.Do(req)
	if err != nil {
		return nil, err
	} else if resp.StatusCode/100 != 2 {
		return nil, ReadError(resp)
	}
	format := resp.Header.Get("Content-Type")
	return NewCursor(resp.Body, format)
}

// Execute executes a query with a POST and returns if any error occurred. It discards the result.
func (q *Querier) Execute(query interface{}, opts ...QueryOption) error {
	opt := q.QueryOptions
	if len(opts) > 0 {
		opt = opt.Clone()
		for _, f := range opts {
			f.apply(&opt)
		}
	}

	req, err := q.c.NewQueryRequest(query, opt)
	if err != nil {
		return err
	}

	resp, err := q.c.Client.Do(req)
	if err != nil {
		return err
	} else if resp.StatusCode/100 != 2 {
		return ReadError(resp)
	}

	format := resp.Header.Get("Content-Type")
	cur, err := NewCursor(resp.Body, format)
	if err != nil {
		return err
	}
	return EachResult(cur, func(ResultSet) error { return nil })
}
