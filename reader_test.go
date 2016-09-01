package influxdb_test

import (
	"bytes"
	"reflect"
	"testing"

	influxdb "github.com/influxdata/influxdb-client"
)

func TestNewReader(t *testing.T) {
	buffer := bytes.NewBufferString(`{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["_internal"]]}]}]}`)
	r, err := influxdb.NewReader(buffer, "json")
	if err != nil {
		t.Fatal(err)
	}

	var result influxdb.Result
	if err := r.Read(&result); err != nil {
		t.Fatal(err)
	}

	want := influxdb.Result{
		Series: []influxdb.Series{{
			Name:    "databases",
			Columns: []string{"name"},
			Values:  [][]interface{}{{"_internal"}},
		}},
	}
	if !reflect.DeepEqual(result.Series, want) {
		t.Errorf("result.Series = %q; want %q", result.Series, want)
	}
	if want := []influxdb.Message(nil); !reflect.DeepEqual(result.Messages, want) {
		t.Errorf("result.Messages = %q; want %q", result.Messages, want)
	}
}

func TestReader_ForEach(t *testing.T) {
	buffer := bytes.NewBufferString(`{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["_internal"]]}]}]}`)
	r, err := influxdb.NewReader(buffer, "json")
	if err != nil {
		t.Fatal(err)
	}

	influxdb.ForEach(r, func(result *influxdb.Result) error {
		want := influxdb.Result{
			Series: []influxdb.Series{{
				Name:    "databases",
				Columns: []string{"name"},
				Values:  [][]interface{}{{"_internal"}},
			}},
		}
		if !reflect.DeepEqual(result.Series, want) {
			t.Errorf("result.Series = %q; want %q", result.Series, want)
		}
		if want := []influxdb.Message(nil); !reflect.DeepEqual(result.Messages, want) {
			t.Errorf("result.Messages = %q; want %q", result.Messages, want)
		}
		return nil
	})
}
