package influxdb_test

import (
	"testing"

	influxdb "github.com/influxdata/influxdb-client"
)

func TestConsistency_String(t *testing.T) {
	tests := []struct {
		consistency influxdb.Consistency
		want        string
	}{
		{
			consistency: influxdb.ConsistencyAll,
			want:        "all",
		},
		{
			consistency: influxdb.ConsistencyOne,
			want:        "one",
		},
		{
			consistency: influxdb.ConsistencyQuorum,
			want:        "quorum",
		},
		{
			consistency: influxdb.ConsistencyAny,
			want:        "any",
		},
	}

	for i, tt := range tests {
		if have := tt.consistency.String(); have != tt.want {
			t.Errorf("%d. String() = %q; want %q", i, have, tt.want)
		}
	}
}
