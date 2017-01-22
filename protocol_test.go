package influxdb_test

import (
	"bytes"
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb-client"
)

func TestLineProtocol_V1(t *testing.T) {
	var buf bytes.Buffer
	p := influxdb.LineProtocol.V1()

	pt := influxdb.Point{
		Name: "cpu",
		Fields: map[string]interface{}{
			"value": float64(5),
		},
	}

	if err := p.Encode(&buf, &pt, influxdb.EncodeOptions{}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if got, want := buf.String(), "cpu value=5\n"; got != want {
		t.Errorf("unexpected protocol output:\n\ngot=%v\nwant=%v\n", got, want)
	}

	buf.Reset()
	pt.Tags = []influxdb.Tag{{Key: "host", Value: "server01"}}

	if err := p.Encode(&buf, &pt, influxdb.EncodeOptions{}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if got, want := buf.String(), "cpu,host=server01 value=5\n"; got != want {
		t.Errorf("unexpected protocol output:\n\ngot=%v\nwant=%v\n", got, want)
	}

	buf.Reset()
	pt.Time = time.Unix(0, 1000)

	if err := p.Encode(&buf, &pt, influxdb.EncodeOptions{}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if got, want := buf.String(), "cpu,host=server01 value=5 1000\n"; got != want {
		t.Errorf("unexpected protocol output:\n\ngot=%v\nwant=%v\n", got, want)
	}
}
