package influxdb

import (
	"io"
	"math"
	"time"
)

// DefaultWriteProtocol is the default write protocol for points to be written in.
// This will always match the write protocol expected by a request created with NewWrite.
const DefaultWriteProtocol = 1

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

// Fields is a mapping of keys to field values. The values must be a float64,
// int64, string, or bool.
type Fields map[string]interface{}

// Value returns a new Fields map with the value key set to the passed in
// interface. This is a convenience function for the common use case of
// inserting a single field with a field key of "value".
func Value(v interface{}) Fields {
	return Fields{"value": v}
}

// Point is a point that can be inserted into a measurement at a time.
type Point struct {
	Name   string
	Tags   Tags
	Fields Fields
	Time   int64
}

// NewPoint creates a new point with the given name and fields.
func NewPoint(name string, fields Fields, timestamp time.Time) Point {
	return NewPointWithTags(name, nil, fields, timestamp)
}

// NewPointWithTags creates a new point with the given name, tags, and fields.
func NewPointWithTags(name string, tags Tags, fields Fields, timestamp time.Time) Point {
	pt := Point{
		Name:   name,
		Tags:   tags,
		Fields: fields,
	}
	pt.SetTime(timestamp)
	return pt
}

// HasTimeSet checks if the time is set on this point.
func (p *Point) HasTimeSet() bool {
	return p.Time != int64(math.MinInt64)
}

// SetTime sets the time on this point. An empty time value will unset the time
// and cause the point to be written without any time (using the current time on the server).
func (p *Point) SetTime(timestamp time.Time) {
	if !timestamp.IsZero() {
		p.Time = timestamp.UnixNano()
	} else {
		// The value of math.MinInt64 is not supported by InfluxDB and is used to signal
		// there is no time set.
		p.Time = int64(math.MinInt64)
	}
}

// Write writes the point to the writer in the specified protocol. If the
// protocol is set to 0, the DefaultWriteProtocol is used. The
// DefaultWriteProtocol will always match the default request created by
// NewWrite.
//
// There is currently only one protocol version and it is version 1. If you
// manually create the request and use this method to write to a request, be
// sure to set this to the write protocol version you want to use. If you are
// using this library and just want more control over writing to the request
// object created by this library, you can just use zero.
func (p *Point) Write(w io.Writer, protocol int) error {
	if protocol == 0 {
		protocol = DefaultWriteProtocol
	}
	return nil
}
