package influxdb_test

import (
	"testing"

	influxdb "github.com/influxdata/influxdb-client"
)

func TestParseUrl(t *testing.T) {
	tests := []struct {
		url   string
		proto string
		addr  string
		path  string
	}{
		{
			url:   "http://localhost:8086",
			proto: "http",
			addr:  "localhost:8086",
		},
		{
			url:   "http://localhost",
			proto: "http",
			addr:  "localhost",
		},
		{
			url:   "http://127.0.0.1:8086",
			proto: "http",
			addr:  "127.0.0.1:8086",
		},
		{
			url:   "http://127.0.0.1",
			proto: "http",
			addr:  "127.0.0.1",
		},
		{
			url:   "https://localhost:8086",
			proto: "https",
			addr:  "localhost:8086",
		},
		{
			url:   "https://localhost",
			proto: "https",
			addr:  "localhost",
		},
		{
			url:   "https://127.0.0.1:8086",
			proto: "https",
			addr:  "127.0.0.1:8086",
		},
		{
			url:   "https://127.0.0.1",
			proto: "https",
			addr:  "127.0.0.1",
		},
		{
			url:   "http://localhost:8086/influxdb",
			proto: "http",
			addr:  "localhost:8086",
			path:  "/influxdb",
		},
		{
			url:   "unix:///var/run/influxdb.sock",
			proto: "unix",
			addr:  "/var/run/influxdb.sock",
		},
		{
			url:  "127.0.0.1:8086",
			addr: "127.0.0.1:8086",
		},
	}

	for i, tt := range tests {
		proto, addr, path, err := influxdb.ParseUrl(tt.url)
		if err != nil {
			t.Errorf("%d. unable to parse url: %q", i, err)
		} else {
			if proto != tt.proto {
				t.Errorf("%d. proto = %q; want %q", i, proto, tt.proto)
			}
			if addr != tt.addr {
				t.Errorf("%d. addr = %q; want %q", i, addr, tt.addr)
			}
			if path != tt.path {
				t.Errorf("%d. path = %q; want %q", i, path, tt.path)
			}
		}
	}
}
