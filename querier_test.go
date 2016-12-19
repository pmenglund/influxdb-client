package influxdb_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	influxdb "github.com/influxdata/influxdb-client"
)

func TestQuerier_Select_Param(t *testing.T) {
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
		if got, want := values.Get("params"), `{"host":"server01"}`; got != want {
			t.Errorf("params = %q; want %q", got, want)
		}
		if got, want := values.Get("q"), "SELECT mean(value) FROM cpu WHERE host = $host"; got != want {
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
	cur, err := querier.Select("SELECT mean(value) FROM cpu WHERE host = $host", influxdb.Param("host", "server01"))
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

func TestQuerier_Select_Params(t *testing.T) {
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
		if got, want := values.Get("params"), `{"host":"server01"}`; got != want {
			t.Errorf("params = %q; want %q", got, want)
		}
		if got, want := values.Get("q"), "SELECT mean(value) FROM cpu WHERE host = $host"; got != want {
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
	cur, err := querier.Select("SELECT mean(value) FROM cpu WHERE host = $host", influxdb.Params(map[string]interface{}{"host": "server01"}))
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
