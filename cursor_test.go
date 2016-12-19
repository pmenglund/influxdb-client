package influxdb_test

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"

	influxdb "github.com/influxdata/influxdb-client"
)

func TestNewCursor_UnknownFormat(t *testing.T) {
	r := ioutil.NopCloser(bytes.NewBuffer(nil))
	_, got := influxdb.NewCursor(r, "unknown format")
	if want := (influxdb.ErrUnknownFormat{Format: "unknown format"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v; want %#v", got, want)
	}
}
