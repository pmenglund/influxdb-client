package influxdb_test

import (
	"testing"

	influxdb "github.com/influxdata/influxdb-client"
)

func TestPrecision_String(t *testing.T) {
	tests := []struct {
		precision influxdb.Precision
		want      string
	}{
		{
			precision: influxdb.PrecisionNanosecond,
			want:      "ns",
		},
		{
			precision: influxdb.PrecisionMicrosecond,
			want:      "u",
		},
		{
			precision: influxdb.PrecisionMillisecond,
			want:      "ms",
		},
		{
			precision: influxdb.PrecisionSecond,
			want:      "s",
		},
		{
			precision: influxdb.PrecisionMinute,
			want:      "m",
		},
		{
			precision: influxdb.PrecisionHour,
			want:      "h",
		},
	}

	for i, tt := range tests {
		if have := tt.precision.String(); have != tt.want {
			t.Errorf("%d. String() = %q; want %q", i, have, tt.want)
		}
	}
}
