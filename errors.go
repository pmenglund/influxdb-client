package influxdb

import (
	"errors"
	"fmt"
)

var (
	// ErrNoFields is returned when attempting to write with no fields.
	ErrNoFields = errors.New("no fields")
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
