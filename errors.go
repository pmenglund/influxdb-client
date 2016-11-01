package influxdb

import (
	"errors"
	"fmt"
)

var (
	// ErrNoFields is returned when attempting to write with no fields.
	ErrNoFields = errors.New("no fields")

	// ErrSeriesTruncated is returned when a series has been truncated and can
	// no longer return more values.
	ErrSeriesTruncated = errors.New("truncated output")
)

type ErrInvalidPrecision struct {
	Precision Precision
}

func (e ErrInvalidPrecision) Error() string {
	return fmt.Sprintf("invalid precision: %s", e.Precision)
}

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
