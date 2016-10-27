package influxdb

import (
	"errors"
	"io"
	"net/http"
)

type Querier struct {
	c       *Client
	Options QueryOptions
}

// Execute executes a query, discards the results, and returns any error that may have happened.
// If the error happened as a result of a statement failing for some reason, the error is wrapped
// in a ResultError.
//
// This is most commonly used with meta queries like CREATE, ALTER, DELETE, and
// DROP queries since the output for those commands don't normally contain any
// useful information.
//
// The parameters are the same as for NewQuery.
func (q *Querier) Execute(query interface{}) error {
	req, err := q.c.NewQuery("POST", query, q.Options)
	if err != nil {
		return err
	}

	resp, err := q.c.Do(req)
	if err != nil {
		return err
	} else if resp.StatusCode != http.StatusOK {
		return errors.New("failure")
	}

	format := resp.Header.Get("Content-Type")
	r, err := NewReader(resp.Body, format)
	if err != nil {
		return err
	}

	for {
		if err := r.Read(nil); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}
