package influxdb_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
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
			want:      "n",
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

func TestNewClient(t *testing.T) {
	c, err := influxdb.NewClient("http://localhost:8086")
	if err != nil {
		t.Fatal(err)
	}

	if c.Proto != "http" {
		t.Errorf("c.Proto = %q; want %q", c.Proto, "http")
	}
	if c.Addr != "localhost:8086" {
		t.Errorf("c.Addr = %q; want %q", c.Addr, "localhost:8086")
	}
	if c.Path != "" {
		t.Errorf("c.Path = %q; want %q", c.Path, "")
	}

	_, err = influxdb.NewClient("invalid url")
	if err == nil {
		t.Errorf("expected error while parsing url")
	}
}

func TestClient_Do(t *testing.T) {
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Path, "/want"; got != want {
			t.Errorf("got %q; want %q", got, want)
		}
		w.WriteHeader(http.StatusNoContent)
		close(done)
	}))
	defer server.Close()

	req, err := http.NewRequest("GET", server.URL+"/want", nil)
	if err != nil {
		t.Fatal(err)
	}

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("StatusCode = %q; want %q", resp.StatusCode, http.StatusNoContent)
	}

	select {
	case <-done:
	default:
		t.Errorf("handler was not triggered")
	}
}

func TestClient_Ping_Success(t *testing.T) {
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Method, "GET"; got != want {
			t.Errorf("Method = %q; want %q", got, want)
		}
		if got, want := r.URL.Path, "/ping"; got != want {
			t.Errorf("Path = %q; want %q", got, want)
		}
		w.WriteHeader(http.StatusNoContent)
		close(done)
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Ping(); err != nil {
		t.Error(err)
	}

	select {
	case <-done:
	default:
		t.Errorf("handler was not triggered")
	}
}

func TestClient_Ping_Failure(t *testing.T) {
	server := httptest.NewServer(nil)
	url := server.URL
	server.Close()

	client, err := influxdb.NewClient(url)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Ping(); err == nil {
		t.Error("expected error, got nil")
	}
}

func TestClient_NewQuery(t *testing.T) {
	client := influxdb.Client{}
	opt := influxdb.QueryOptions{
		Database:        "db0",
		RetentionPolicy: "rp0",
		Chunked:         true,
		ChunkSize:       1024,
		Pretty:          true,
		Format:          "json",
	}
	req, err := client.NewQuery("POST", "SELECT * FROM cpu", &opt)
	if err != nil {
		t.Fatal(err)
	}

	if req.Method != "POST" {
		t.Errorf("Method = %q; want %q", req.Method, "POST")
	}

	// Query options are put into the URL so they show up in the server log to aid with debugging.
	values := req.URL.Query()
	if got, want := values.Get("db"), "db0"; got != want {
		t.Errorf("db = %q; want %q", got, want)
	}
	if got, want := values.Get("rp"), "rp0"; got != want {
		t.Errorf("rp = %q; want %q", got, want)
	}
	if got, want := values.Get("chunked"), "true"; got != want {
		t.Errorf("chunked = %q; want %q", got, want)
	}
	if got, want := values.Get("chunk_size"), "1024"; got != want {
		t.Errorf("chunk_size = %q; want %q", got, want)
	}
	if got, want := values.Get("pretty"), "true"; got != want {
		t.Errorf("pretty = %q; want %q", got, want)
	}

	if got, want := req.Header.Get("Content-Type"), "application/x-www-form-urlencoded"; got != want {
		t.Errorf("Content-Type = %q; want %q", got, want)
	}

	if req.Body == nil {
		t.Fatal("expected request to have a body")
	}

	out, err := ioutil.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}

	body, err := url.ParseQuery(string(out))
	if err != nil {
		t.Fatal(err)
	}

	// The query itself is put into the body so it can be an arbitary length
	// without any limitations on GET.
	if got, want := body.Get("q"), "SELECT * FROM cpu"; got != want {
		t.Errorf("q = %q; want %q", got, want)
	}
}

func TestClient_Select(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Method, "GET"; got != want {
			t.Errorf("Method = %q; want %q", got, want)
		}

		values := r.URL.Query()
		if got, want := values.Get("db"), "db0"; got != want {
			t.Errorf("db = %q; want %q", got, want)
		}
		if got, want := values.Get("rp"), ""; got != want {
			t.Errorf("rp = %q; want %q", got, want)
		}
		if got, want := values.Get("q"), "SELECT mean(value) FROM cpu"; got != want {
			t.Errorf("q = %q; want %q", got, want)
		}

		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"results":[{"name":"cpu","columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",5]]}]}`)
		w.Write([]byte("\n"))
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	opt := influxdb.QueryOptions{Database: "db0"}
	r, err := client.Select("SELECT mean(value) FROM cpu", &opt)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	result := influxdb.Result{}
	if err := r.Read(&result); err != nil {
		t.Fatal(err)
	}

	if len(result.Series) != 1 {
		t.Fatalf("number of series = %q; want %q", len(result.Series), 1)
	}

	series := result.Series[0]
	if series.Name != "cpu" {
		t.Errorf("Name = %q; want %q", series.Name, "cpu")
	}
	if got, want := series.Columns, []string{"time", "mean"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Columns = %q; want %q", got, want)
	}
	if len(series.Values) != 1 {
		t.Errorf("number of values = %q; want %q", len(series.Values), 1)
	} else {
		values := series.Values[0]
		if len(values) != 2 {
			t.Errorf("length of values slice = %q; want %q", len(values), 2)
		} else {
			if got, want := values[0].(string), "1970-01-01T00:00:00Z"; got != want {
				t.Errorf("time value = %q; want %q", got, want)
			}
			if got, want := values[1].(float64), 5.0; got != want {
				t.Errorf("mean value = %q; want %q", got, want)
			}
		}
	}

	if err := r.Read(&result); err != io.EOF {
		t.Errorf("got error %q; want %q", err, io.EOF)
	}
}

func TestClient_Execute_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Method, "GET"; got != want {
			t.Errorf("Method = %q; want %q", got, want)
		}

		values := r.URL.Query()
		if got, want := values.Get("q"), "CREATE DATABASE db0"; got != want {
			t.Errorf("q = %q; want %q", got, want)
		}

		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"results":[{}]}`)
		w.Write([]byte("\n"))
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Execute("CREATE DATABASE db0", nil); err != nil {
		t.Errorf("got error %q; want nil", err)
	}
}

func TestClient_Execute_Failure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Method, "GET"; got != want {
			t.Errorf("Method = %q; want %q", got, want)
		}

		values := r.URL.Query()
		if got, want := values.Get("q"), "CREATE DATABASE db0"; got != want {
			t.Errorf("q = %q; want %q", got, want)
		}

		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"results":[{"error":"expected err"}]}`)
		w.Write([]byte("\n"))
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Execute("CREATE DATABASE db0", nil); err == nil {
		t.Errorf("expected error")
	} else if e, ok := err.(*influxdb.ResultError); !ok {
		t.Errorf("got error type %T; want %T", err, e)
	} else if e.Err != "expected err" {
		t.Errorf("error message %q; want %q", e.Err, "expected err")
	}
}

func TestClient_NewWrite(t *testing.T) {
	buf := bytes.NewBufferString("expected body")

	client := influxdb.Client{}
	opt := influxdb.WriteOptions{
		Database:        "db0",
		RetentionPolicy: "rp0",
		Precision:       influxdb.PrecisionSecond,
		Consistency:     influxdb.ConsistencyOne,
	}
	req, err := client.NewWrite(buf, &opt)
	if err != nil {
		t.Fatal(err)
	}

	if req.Method != "POST" {
		t.Errorf("Method = %q; want %q", req.Method, "POST")
	}

	// Query options are put into the URL so they show up in the server log to aid with debugging.
	values := req.URL.Query()
	if got, want := values.Get("db"), "db0"; got != want {
		t.Errorf("db = %q; want %q", got, want)
	}
	if got, want := values.Get("rp"), "rp0"; got != want {
		t.Errorf("rp = %q; want %q", got, want)
	}
	if got, want := values.Get("precision"), "s"; got != want {
		t.Errorf("precision = %q; want %q", got, want)
	}
	if got, want := values.Get("consistency"), "one"; got != want {
		t.Errorf("consistency = %q; want %q", got, want)
	}

	if req.Body == nil {
		t.Fatal("expected request to have a body")
	}

	out, err := ioutil.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := string(out), "expected body"; got != want {
		t.Errorf("body = %q; want %q", got, want)
	}
}
