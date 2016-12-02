package influxdb_test

import (
	"sort"
	"testing"

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
