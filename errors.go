package influxdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

var (
	// ErrStop can be returned by the EachXXX methods to cause them to return early with no error.
	ErrStop = errors.New("stop")

	// ErrNoFields is returned when attempting to write with no fields.
	ErrNoFields = errors.New("no fields")

	// ErrSeriesTruncated is returned when a series has been truncated and can
	// no longer return more values.
	ErrSeriesTruncated = errors.New("truncated output")
)

type ErrPing struct {
	Cause error
}

func (e ErrPing) Error() string {
	return fmt.Sprintf("ping failed: %s", e.Cause)
}

type ErrUnknownFormat struct {
	Format string
}

func (e ErrUnknownFormat) Error() string {
	return fmt.Sprintf("unknown format: %s", e.Format)
}

type ErrResult struct {
	Err string
}

func (e ErrResult) Error() string {
	return e.Err
}

// ReadError reads the HTTP response for an error and returns it.
// It currently only supports errors sent back as JSON.
func ReadError(resp *http.Response) error {
	out, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("unknown http error: %s", resp.StatusCode)
	}

	msg := string(out)
	switch resp.Header.Get("Content-Type") {
	case "application/json":
		var jsonErr struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(out, &jsonErr); err == nil {
			// Ignore any errors from parsing the JSON from the server.
			// The server may have just sent a bad message and we don't want to mask that.
			msg = jsonErr.Error
		}
	}
	return errors.New(msg)
}
