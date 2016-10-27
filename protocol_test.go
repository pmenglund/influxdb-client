package influxdb_test

import (
	"bytes"
	"os"
	"testing"

	influxdb "github.com/influxdata/influxdb-client"
)

func TestLineProtocol_V1(t *testing.T) {
	enc := influxdb.LineProtocol.V1()
	points := []influxdb.Point{
		{
			Name: "cpu",
			Tags: []influxdb.Tag{
				{Key: "host", Value: "server01"},
				{Key: "region", Value: "uswest"},
			},
			Fields: influxdb.Value(2.0),
		},
	}

	var buf bytes.Buffer
	for _, pt := range points {
		if err := enc.Encode(&buf, &pt); err != nil {
			t.Fatal(err)
		}
	}

	want := `cpu,host=server01,region=uswest value=2
`
	if got := buf.String(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func ExampleLineProtocol_V1() {
	enc := influxdb.LineProtocol.V1()
	points := []influxdb.Point{
		{
			Name: "cpu",
			Tags: []influxdb.Tag{
				{Key: "host", Value: "server01"},
				{Key: "region", Value: "uswest"},
			},
			Fields: influxdb.Value(2.0),
		},
	}

	for _, pt := range points {
		if err := enc.Encode(os.Stdout, &pt); err != nil {
			panic(err)
		}
	}
	// Output: cpu,host=server01,region=uswest value=2
}
