package influxdb

import "time"

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
	return Point{
		Name:   name,
		Tags:   tags,
		Fields: fields,
		Time:   timestamp.UnixNano(),
	}
}