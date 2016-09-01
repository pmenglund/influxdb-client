package influxdb_test

import (
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb-client"
)

func TestTags_Sort(t *testing.T) {
	tags := []influxdb.Tag{
		{Key: "region", Value: "useast"},
		{Key: "host", Value: "server01"},
	}
	sort.Sort(influxdb.Tags(tags))

	if tags[0].Key != "host" {
		t.Errorf("have %q, want %q", tags[0].Key, "host")
	}
	if tags[0].Value != "server01" {
		t.Errorf("have %q, want %q", tags[0].Value, "server01")
	}
	if tags[1].Key != "region" {
		t.Errorf("have %q, want %q", tags[0].Key, "region")
	}
	if tags[1].Value != "useast" {
		t.Errorf("have %q, want %q", tags[0].Value, "useast")
	}
}

func TestValue(t *testing.T) {
	fields := influxdb.Value(2.0)
	if want := influxdb.Fields(map[string]interface{}{"value": 2.0}); !reflect.DeepEqual(fields, want) {
		t.Errorf("have %q, want %q", fields, want)
	}
}

func TestNewPoint(t *testing.T) {
	now := time.Now()
	pt := influxdb.NewPoint("cpu", influxdb.Value(2.0), now)
	if pt.Name != "cpu" {
		t.Errorf("pt.Name = %q; want %q", pt.Name, "cpu")
	}
	if want := influxdb.Tags(nil); !reflect.DeepEqual(pt.Tags, want) {
		t.Errorf("pt.Tags = %q; want %q", pt.Tags, want)
	}
	if want := influxdb.Fields(map[string]interface{}{"value": 2.0}); !reflect.DeepEqual(pt.Fields, want) {
		t.Errorf("pt.Fields = %q; want %q", pt.Fields, want)
	}
	if pt.Time != now.UnixNano() {
		t.Errorf("pt.Time = %q; want %q", pt.Time, now.UnixNano())
	}
}

func TestNewPointWithTags(t *testing.T) {
	now := time.Now()
	pt := influxdb.NewPointWithTags("cpu",
		[]influxdb.Tag{{Key: "host", Value: "server01"}},
		influxdb.Value(2.0), now)
	if pt.Name != "cpu" {
		t.Errorf("pt.Name = %q; want %q", pt.Name, "cpu")
	}
	if want := influxdb.Tags([]influxdb.Tag{{Key: "host", Value: "server01"}}); !reflect.DeepEqual(pt.Tags, want) {
		t.Errorf("pt.Tags = %q; want %q", pt.Tags, want)
	}
	if want := influxdb.Fields(map[string]interface{}{"value": 2.0}); !reflect.DeepEqual(pt.Fields, want) {
		t.Errorf("pt.Fields = %q; want %q", pt.Fields, want)
	}
	if pt.Time != now.UnixNano() {
		t.Errorf("pt.Time = %q; want %q", pt.Time, now.UnixNano())
	}
}

func TestPoint_HasTimeSet(t *testing.T) {
	pt := influxdb.NewPoint("cpu", influxdb.Value(0), time.Time{})
	if pt.HasTimeSet() {
		t.Error("expected time to not be set")
	}

	pt = influxdb.NewPoint("cpu", influxdb.Value(0), time.Now())
	if !pt.HasTimeSet() {
		t.Error("expected time to be set")
	}
}

func TestPoint_SetTime(t *testing.T) {
	now := time.Now()
	pt := influxdb.NewPoint("cpu", influxdb.Value(0), time.Time{})
	if pt.Time != int64(math.MinInt64) {
		t.Errorf("pt.Time = %q; want %q", pt.Time, int64(math.MinInt64))
	}

	pt.SetTime(now)
	if pt.Time != now.UnixNano() {
		t.Errorf("pt.Time = %q; want %q", pt.Time, now.UnixNano())
	}

	pt.SetTime(time.Time{})
	if pt.Time != int64(math.MinInt64) {
		t.Errorf("pt.Time = %q; want %q", pt.Time, int64(math.MinInt64))
	}
}