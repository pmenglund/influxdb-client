package influxdb_test

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	influxdb "github.com/influxdata/influxdb-client"
)

func TestNewClient(t *testing.T) {
	c, err := influxdb.NewClient("http://user:pass@localhost:8086/foo")
	if err != nil {
		t.Fatal(err)
	}

	if c.Proto != "http" {
		t.Errorf("c.Proto = %q; want %q", c.Proto, "http")
	}
	if c.Addr != "localhost:8086" {
		t.Errorf("c.Addr = %q; want %q", c.Addr, "localhost:8086")
	}
	if c.Path != "/foo" {
		t.Errorf("c.Path = %q; want %q", c.Path, "/foo")
	}
	if exp := (influxdb.Auth{Username: "user", Password: "pass"}); c.Auth == nil || *c.Auth != exp {
		t.Errorf("c.Auth = %q; want %q", c.Auth, exp)
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
		w.Header().Set("X-Influxdb-Version", "v1.0.0")
		w.WriteHeader(http.StatusNoContent)
		close(done)
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	serverInfo, err := client.Ping()
	if err != nil {
		t.Error(err)
	}

	if got, want := "v1.0.0", serverInfo.Version; got != want {
		t.Errorf("Version = %q; want %q", got, want)
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

	if _, err := client.Ping(); err == nil {
		t.Error("expected error, got nil")
	}
}

func TestClient_NewQueryRequest_String(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Method = %q; want %q", r.Method, "POST")
		}
		if r.URL.Path != "/query" {
			t.Errorf("Path = %q; want %q", r.URL.Path, "/query")
		}

		// Query options are put into the URL so they show up in the server log to aid with debugging.
		values := r.URL.Query()
		if got, want := values.Get("db"), "db0"; got != want {
			t.Errorf("db = %q; want %q", got, want)
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
		if got, want := values.Get("q"), "SELECT * FROM cpu"; got != want {
			t.Errorf("q = %q; want %q", got, want)
		}
		if got, want := r.Header.Get("Content-Type"), ""; got != want {
			t.Errorf("Content-Type = %q; want %q", got, want)
		}
		if got, want := r.Header.Get("Accept"), "application/json"; got != want {
			t.Errorf("Accept = %q; want %q", got, want)
		}
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	opt := influxdb.QueryOptions{
		Database:  "db0",
		Chunked:   true,
		ChunkSize: 1024,
		Pretty:    true,
		Format:    "json",
	}
	req, err := client.NewQueryRequest("SELECT * FROM cpu", opt)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClient_NewQueryRequest_ioReader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Method = %q; want %q", r.Method, "POST")
		}
		if r.URL.Path != "/query" {
			t.Errorf("Path = %q; want %q", r.URL.Path, "/query")
		}

		// Query options are put into the URL so they show up in the server log to aid with debugging.
		values := r.URL.Query()
		if got, want := values.Get("db"), "db0"; got != want {
			t.Errorf("db = %q; want %q", got, want)
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
		if got, want := r.Header.Get("Content-Type"), "multipart/form-data"; !strings.HasPrefix(got, want+";") {
			t.Errorf("Content-Type = %q; want %q", got, want)
		}
		if got, want := r.Header.Get("Accept"), "application/json"; got != want {
			t.Errorf("Accept = %q; want %q", got, want)
		}

		f, _, err := r.FormFile("q")
		if err != nil {
			t.Errorf("unexpected error reading file: %v", err)
		} else {
			q, err := ioutil.ReadAll(f)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			} else if got, want := string(q), "SELECT * FROM cpu"; got != want {
				t.Errorf("q = %q; want %q", got, want)
			}
			f.Close()
		}
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	opt := influxdb.QueryOptions{
		Database:  "db0",
		Chunked:   true,
		ChunkSize: 1024,
		Pretty:    true,
		Format:    "json",
	}
	req, err := client.NewQueryRequest(strings.NewReader("SELECT * FROM cpu"), opt)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClient_NewReadonlyQueryRequest_String(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Method = %q; want %q", r.Method, "GET")
		}
		if r.URL.Path != "/query" {
			t.Errorf("Path = %q; want %q", r.URL.Path, "/query")
		}

		// Query options are put into the URL so they show up in the server log to aid with debugging.
		values := r.URL.Query()
		if got, want := values.Get("db"), "db0"; got != want {
			t.Errorf("db = %q; want %q", got, want)
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
		if got, want := values.Get("q"), "SELECT * FROM cpu"; got != want {
			t.Errorf("q = %q; want %q", got, want)
		}
		if got, want := r.Header.Get("Content-Type"), ""; got != want {
			t.Errorf("Content-Type = %q; want %q", got, want)
		}
		if got, want := r.Header.Get("Accept"), "application/json"; got != want {
			t.Errorf("Accept = %q; want %q", got, want)
		}
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	opt := influxdb.QueryOptions{
		Database:  "db0",
		Chunked:   true,
		ChunkSize: 1024,
		Pretty:    true,
		Format:    "json",
	}
	req, err := client.NewReadonlyQueryRequest("SELECT * FROM cpu", opt)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClient_NewReadonlyQueryRequest_ioReader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Method = %q; want %q", r.Method, "GET")
		}
		if r.URL.Path != "/query" {
			t.Errorf("Path = %q; want %q", r.URL.Path, "/query")
		}

		// Query options are put into the URL so they show up in the server log to aid with debugging.
		values := r.URL.Query()
		if got, want := values.Get("db"), "db0"; got != want {
			t.Errorf("db = %q; want %q", got, want)
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
		if got, want := r.Header.Get("Content-Type"), "application/x-www-form-urlencoded"; got != want {
			t.Errorf("Content-Type = %q; want %q", got, want)
		}
		if got, want := r.Header.Get("Accept"), "application/json"; got != want {
			t.Errorf("Accept = %q; want %q", got, want)
		}

		out, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}

		body, err := url.ParseQuery(string(out))
		if err != nil {
			t.Fatal(err)
		}

		if got, want := body.Get("q"), "SELECT * FROM cpu"; got != want {
			t.Errorf("q = %q; want %q", got, want)
		}
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	opt := influxdb.QueryOptions{
		Database:  "db0",
		Chunked:   true,
		ChunkSize: 1024,
		Pretty:    true,
		Format:    "json",
	}
	req, err := client.NewReadonlyQueryRequest(strings.NewReader("SELECT * FROM cpu"), opt)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
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
		io.WriteString(w, `{"results":[{"series":[{"name":"cpu","columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",5]]}]}]}`)
		w.Write([]byte("\n"))
	}))
	defer server.Close()

	client, err := influxdb.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	querier := client.Querier()
	querier.Database = "db0"
	cur, err := querier.Select("SELECT mean(value) FROM cpu")
	if err != nil {
		t.Fatal(err)
	}
	defer cur.Close()

	result, err := cur.NextSet()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	series, err := result.NextSeries()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, exp := series.Name(), "cpu"; got != exp {
		t.Fatalf("Name = %q; want %q", got, exp)
	}
	if got, want := series.Columns(), []string{"time", "mean"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Columns = %q; want %q", got, want)
	}

	n, _ := series.Len()
	got := make([][]interface{}, 0, n)
	influxdb.EachRow(series, func(row influxdb.Row) error {
		got = append(got, row.Values())
		return nil
	})

	exp := [][]interface{}{
		[]interface{}{"1970-01-01T00:00:00Z", float64(5)},
	}
	if !reflect.DeepEqual(got, exp) {
		t.Fatalf("Values = %q; want %q", got, exp)
	}

	// There should not be another series.
	if _, err = result.NextSeries(); err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}

	// There should not be another result.
	if _, err = cur.NextSet(); err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClient_Execute_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Method, "POST"; got != want {
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

	if err := client.Execute("CREATE DATABASE db0"); err != nil {
		t.Errorf("got error %q; want nil", err)
	}
}

func TestClient_Execute_Failure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Method, "POST"; got != want {
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

	if err := client.Execute("CREATE DATABASE db0"); err == nil {
		t.Errorf("expected error")
	} else if e, ok := err.(influxdb.ErrResult); !ok {
		t.Errorf("got error type %T; want %T", err, e)
	} else if e.Err != "expected err" {
		t.Errorf("error message %q; want %q", e.Err, "expected err")
	}
}
