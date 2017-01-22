package influxdb

import (
	"bytes"
	"time"
)

// Tag is a key/value pair of strings that is indexed when inserted into a measurement.
type Tag struct {
	Key   string
	Value string
}

// Tags is a list of Tag structs. For optimal efficiency, this should be inserted
// into InfluxDB in a sorted order and should only contain unique values.
type Tags []Tag

func (a Tags) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a Tags) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Tags) Len() int           { return len(a) }

func (a Tags) String() string {
	var buf bytes.Buffer
	for i, t := range a {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(t.Key)
		buf.WriteString("=")
		buf.WriteString(t.Value)
	}
	return buf.String()
}

// Point represents a point to be written.
type Point struct {
	Name   string
	Tags   Tags
	Fields map[string]interface{}
	Time   time.Time
}
