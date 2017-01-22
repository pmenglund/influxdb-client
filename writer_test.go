package influxdb_test

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb-client"
)

func TestWriter_WritePoint(t *testing.T) {
	protocol := influxdb.DefaultWriteProtocol
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Method, "POST"; got != want {
			t.Errorf("Method = %q; want %q", got, want)
		}

		values := r.URL.Query()
		if got, want := values.Get("db"), "db0"; got != want {
			t.Errorf("db = %q; want %q", got, want)
		}
		if got, want := values.Get("rp"), "rp0"; got != want {
			t.Errorf("rp = %q; want %q", got, want)
		}
		if got, want := r.Header.Get("Content-Type"), protocol.ContentType(); got != want {
			t.Errorf("Content-Type = %q; want %q", got, want)
		}

		data, _ := ioutil.ReadAll(r.Body)
		if got, want := string(data), "cpu,host=server01 value=5\n"; got != want {
			t.Errorf("body = %q; want %q", got, want)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	writer := client.Writer()
	writer.Database = "db0"
	writer.RetentionPolicy = "rp0"

	pt := influxdb.Point{
		Name:   "cpu",
		Tags:   influxdb.Tags{{Key: "host", Value: "server01"}},
		Fields: map[string]interface{}{"value": 5.0},
	}
	if _, err := writer.WritePoint(pt); err != nil {
		t.Fatal(err)
	}
}

// This tests if io.Copy works with the Writer.
func TestWriter_Copy(t *testing.T) {
	protocol := influxdb.DefaultWriteProtocol
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Method, "POST"; got != want {
			t.Errorf("Method = %q; want %q", got, want)
		}

		values := r.URL.Query()
		if got, want := values.Get("db"), "db0"; got != want {
			t.Errorf("db = %q; want %q", got, want)
		}
		if got, want := values.Get("rp"), "rp0"; got != want {
			t.Errorf("rp = %q; want %q", got, want)
		}
		if got, want := r.Header.Get("Content-Type"), protocol.ContentType(); got != want {
			t.Errorf("Content-Type = %q; want %q", got, want)
		}

		data, _ := ioutil.ReadAll(r.Body)
		if got, want := string(data), "cpu,host=server01 value=5\n"; got != want {
			t.Errorf("body = %q; want %q", got, want)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	writer := client.Writer()
	writer.Database = "db0"
	writer.RetentionPolicy = "rp0"

	r := strings.NewReader("cpu,host=server01 value=5\n")
	if _, err := io.Copy(writer, r); err != nil {
		t.Fatal(err)
	}
}

func TestWriter_ConsistencyAndPrecision(t *testing.T) {
	protocol := influxdb.DefaultWriteProtocol
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Method, "POST"; got != want {
			t.Errorf("Method = %q; want %q", got, want)
		}

		values := r.URL.Query()
		if got, want := values.Get("db"), "db0"; got != want {
			t.Errorf("db = %q; want %q", got, want)
		}
		if got, want := values.Get("rp"), "rp0"; got != want {
			t.Errorf("rp = %q; want %q", got, want)
		}
		if got, want := values.Get("consistency"), "any"; got != want {
			t.Errorf("consistency = %q; want %q", got, want)
		}
		if got, want := values.Get("precision"), "s"; got != want {
			t.Errorf("precision = %q; want %q", got, want)
		}
		if got, want := r.Header.Get("Content-Type"), protocol.ContentType(); got != want {
			t.Errorf("Content-Type = %q; want %q", got, want)
		}

		data, _ := ioutil.ReadAll(r.Body)
		if got, want := string(data), "cpu,host=server01 value=5 10\n"; got != want {
			t.Errorf("body = %q; want %q", got, want)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	writer := client.Writer()
	writer.Database = "db0"
	writer.RetentionPolicy = "rp0"
	writer.Consistency = influxdb.ConsistencyAny
	writer.Precision = influxdb.PrecisionSecond

	pt := influxdb.Point{
		Name:   "cpu",
		Tags:   influxdb.Tags{{Key: "host", Value: "server01"}},
		Fields: map[string]interface{}{"value": 5.0},
		Time:   time.Unix(10, 0),
	}
	if _, err := writer.WritePoint(pt); err != nil {
		t.Fatal(err)
	}
}
