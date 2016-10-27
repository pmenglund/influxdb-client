package influxdb

import (
	"errors"
	"io"
	"math"
	"time"
)

// ZeroTime is the time that indicates there is no time set.
const ZeroTime = int64(math.MinInt64)

// ErrNoFields is returned when attempting to write with no fields.
var ErrNoFields = errors.New("no fields")

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

// Write writes the tags in line protocol format. This outputs the beginning
// comma if there is at least one tag. If there are no tags, this writes
// nothing.
func (a Tags) Write(w io.Writer, protocol int) error {
	if len(a) == 0 {
		return nil
	}
	return nil
}

// Fields is a mapping of keys to field values. The values must be a float64,
// int64, string, or bool.
type Fields map[string]interface{}

// Write writes the fields in line protocol format. An error is returned if
// there are zero fields.
func (f Fields) Write(w io.Writer, protocol int) error {
	if len(f) == 0 {
		return ErrNoFields
	}
	return nil
}

// Value returns a new Fields map with the value key set to the passed in
// interface. This is a convenience function for the common use case of
// inserting a single field with a field key of "value".
func Value(v interface{}) Fields {
	return Fields{"value": v}
}

// Time abstracts away different ways of specifying the timestamp.
type Time interface {
	UnixNano() int64
	IsZero() bool
}

func TimeValue(t time.Time, precision Precision) Time {
	return timeValue{
		t: t,
		p: precision,
	}
}

type timeValue struct {
	t time.Time
	p Precision
}

func (t timeValue) UnixNano() int64 {
	value := t.UnixNano()
	switch t.p {
	case PrecisionHour:
		value /= 60
		fallthrough
	case PrecisionMinute:
		value /= 60
		fallthrough
	case PrecisionSecond:
		value /= 1000
		fallthrough
	case PrecisionMillisecond:
		value /= 1000
		fallthrough
	case PrecisionMicrosecond:
		value /= 1000
	}
	return value
}

func (t timeValue) IsZero() bool {
	return t.t.IsZero()
}

// Point is a point that can be inserted into a measurement at a time.
type Point struct {
	Name   string
	Tags   Tags
	Fields Fields
	Time   Time
}

// NewPoint creates a new point with the given name and fields.
func NewPoint(name string, fields Fields, timestamp Time) Point {
	return NewPointWithTags(name, nil, fields, timestamp)
}

// NewPointWithTags creates a new point with the given name, tags, and fields.
func NewPointWithTags(name string, tags Tags, fields Fields, timestamp Time) Point {
	return Point{
		Name:   name,
		Tags:   tags,
		Fields: fields,
		Time:   timestamp,
	}
}
