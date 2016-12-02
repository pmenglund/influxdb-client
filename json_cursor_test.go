package influxdb_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb-client"
)

func TestCursor_JSON_Basic(t *testing.T) {
	r := strings.NewReader(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2010-01-01T00:00:00Z",2],["2010-01-01T00:00:10Z",3]]}]}]}`)
	cur, err := influxdb.NewCursor(ioutil.NopCloser(r), "json")
	if err != nil {
		t.Fatal(err)
	}

	result, err := cur.NextSet()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if got, want := result.Columns(), []string{"time", "value"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v; want %#v", got, want)
	}

	series, err := result.NextSeries()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	sz, complete := series.Len()
	if sz != 2 {
		t.Fatalf("got %#v; want %#v", sz, 2)
	} else if !complete {
		t.Fatal("expected complete to be true, was false")
	}

	if got, want := series.Name(), "cpu"; got != want {
		t.Fatalf("got %#v; want %#v", got, want)
	} else if got, want := series.Tags(), influxdb.Tags(nil); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v; want %#v", got, want)
	}

	if got, err := series.NextRow(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	} else if want := []interface{}{"2010-01-01T00:00:00Z", float64(2)}; !reflect.DeepEqual(got.Values(), want) {
		t.Fatalf("got %#v; want %#v", got.Values(), want)
	}

	if got, err := series.NextRow(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	} else if want := []interface{}{"2010-01-01T00:00:10Z", float64(3)}; !reflect.DeepEqual(got.Values(), want) {
		t.Fatalf("got %#v; want %#v", got.Values(), want)
	}

	if _, err := series.NextRow(); err != io.EOF {
		t.Fatalf("expected %v, got %v", io.EOF, err)
	}
	if _, err := result.NextSeries(); err != io.EOF {
		t.Fatalf("expected %v, got %v", io.EOF, err)
	}
	if _, err := cur.NextSet(); err != io.EOF {
		t.Fatalf("expected %v, got %v", io.EOF, err)
	}
}

func TestCursor_JSON_ResultError(t *testing.T) {
	r := strings.NewReader(`{"results":[{"error":"expected err"}]}`)
	cur, err := influxdb.NewCursor(ioutil.NopCloser(r), "json")
	if err != nil {
		t.Fatal(err)
	}

	_, err = cur.NextSet()
	if want := (influxdb.ErrResult{Err: "expected err"}); err != want {
		t.Fatalf("got error %#v; want %#v", err, want)
	}

	r = strings.NewReader(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2010-01-01T00:00:00Z",2]],"partial":true}],"partial":true}]}{"results":[{"error":"expected err"}]}`)
	cur, err = influxdb.NewCursor(ioutil.NopCloser(r), "json")
	if err != nil {
		t.Fatal(err)
	}

	result, err := cur.NextSet()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	series, err := result.NextSeries()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if got, err := series.NextRow(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	} else if want := []interface{}{"2010-01-01T00:00:00Z", float64(2)}; !reflect.DeepEqual(got.Values(), want) {
		t.Fatalf("got %#v; want %#v", got.Values(), want)
	}

	// This should follow the partial series to the next returned result which should have an error.
	_, err = series.NextRow()
	if want := (influxdb.ErrResult{Err: "expected err"}); err != want {
		t.Fatalf("got error %#v; want %#v", err, want)
	}
}

func TestCursor_JSON_TruncatedSeries(t *testing.T) {
	r := strings.NewReader(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[["2010-01-01T00:00:00Z",2]],"partial":true}]}]}`)
	cur, err := influxdb.NewCursor(ioutil.NopCloser(r), "json")
	if err != nil {
		t.Fatal(err)
	}

	result, err := cur.NextSet()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	series, err := result.NextSeries()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := series.NextRow(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// We should get an ErrSeriesTruncated from NextRow.
	if _, got := series.NextRow(); got != influxdb.ErrSeriesTruncated {
		t.Fatalf("got error %#v; want %#v", got, influxdb.ErrSeriesTruncated)
	}
}

func TestCursor_JSON_Row(t *testing.T) {
	ts := mustParseTime("2010-01-01T00:00:00Z")
	r := strings.NewReader(fmt.Sprintf(`{"results":[{"series":[{"name":"cpu","columns":["time","value"],"values":[[%d,2]]}]}]}`, ts.UnixNano()))
	cur, err := influxdb.NewCursor(ioutil.NopCloser(r), "json")
	if err != nil {
		t.Fatal(err)
	}

	result, err := cur.NextSet()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	series, err := result.NextSeries()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	row, err := series.NextRow()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, want := row.Time(), ts; got != want {
		t.Fatalf("got %#v; want %#v", got, want)
	}

	if got, want := row.ValueByName("value"), float64(2); got != want {
		t.Fatalf("got %#v; want %#v", got, want)
	}

	if got, want := row.Value(1), float64(2); got != want {
		t.Fatalf("got %#v; want %#v", got, want)
	}
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}
