package influxdb

import "fmt"

type ErrInvalidPrecision struct {
	Precision Precision
}

func (e ErrInvalidPrecision) Error() string {
	return fmt.Sprintf("invalid precision: %s", e.Precision)
}
