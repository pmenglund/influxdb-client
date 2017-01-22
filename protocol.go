package influxdb

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// Protocol implements a protocol encoder.
type Protocol interface {
	// Encode encodes the Point into the io.Writer.
	Encode(w io.Writer, pt *Point, opt EncodeOptions) error

	// ContentType returns the Content Type of this protocol format.
	ContentType() string
}

// LineProtocol holds the factory methods for different versions of the line protocol.
var LineProtocol = struct {
	V1 func() Protocol
}{
	V1: func() Protocol { return (*lineProtocolV1)(nil) },
}

var (
	// DefaultWriteProtocol is the default write protocol for points to be written in.
	// This will always match the write protocol expected by a request created with NewWrite.
	DefaultWriteProtocol = LineProtocol.V1()

	// DefaultWriteType is the default content type of the default write protocol.
	DefaultWriteType = DefaultWriteProtocol.ContentType()
)

// EncodeOptions keeps the extra options that maybe used when encoding points.
// There is no guarantee that all options here are used by a protocol.
type EncodeOptions struct {
	Precision Precision
}

// Encode encodes a point using the DefaultWriteProtocol.
func Encode(w io.Writer, pt *Point) error {
	return DefaultWriteProtocol.Encode(w, pt, EncodeOptions{})
}

type lineProtocolV1 struct{}

func (*lineProtocolV1) Encode(w io.Writer, pt *Point, opt EncodeOptions) error {
	if len(pt.Fields) == 0 {
		return ErrNoFields
	}

	precisionFactor := int64(1)
	if opt.Precision != "" {
		switch opt.Precision {
		case PrecisionHour:
			precisionFactor = int64(time.Hour)
		case PrecisionMinute:
			precisionFactor = int64(time.Minute)
		case PrecisionSecond:
			precisionFactor = int64(time.Second)
		case PrecisionMillisecond:
			precisionFactor = int64(time.Millisecond)
		case PrecisionMicrosecond:
			precisionFactor = int64(time.Microsecond)
		case PrecisionNanosecond:
			// no change in the precision factor is needed
		}
	}

	io.WriteString(w, escapeMeasurement(pt.Name))
	if len(pt.Tags) > 0 {
		for _, t := range pt.Tags {
			io.WriteString(w, ",")
			io.WriteString(w, escapeTag(t.Key))
			io.WriteString(w, "=")
			io.WriteString(w, escapeTag(t.Value))
		}
	}
	io.WriteString(w, " ")

	i := 0
	for k, v := range pt.Fields {
		if i > 0 {
			io.WriteString(w, ",")
		}
		io.WriteString(w, escapeString(k))
		io.WriteString(w, "=")

		value, err := formatValue(v)
		if err != nil {
			return err
		}
		io.WriteString(w, value)
		i++
	}
	if !pt.Time.IsZero() {
		io.WriteString(w, " ")
		ts := pt.Time.UnixNano() / precisionFactor
		io.WriteString(w, strconv.FormatInt(ts, 10))
	}
	io.WriteString(w, "\n")
	return nil
}

func (*lineProtocolV1) ContentType() string {
	return "application/x-influxdb-line-protocol-v1"
}

type escapeSequence struct {
	s   string
	esc string
}

var (
	measurementEscapeCodes = []escapeSequence{
		{s: `,`, esc: `\,`},
		{s: ` `, esc: `\ `},
	}

	tagEscapeCodes = []escapeSequence{
		{s: `,`, esc: `\,`},
		{s: ` `, esc: `\ `},
		{s: `=`, esc: `\=`},
	}

	stringEscapeCodes = []escapeSequence{
		{s: `\`, esc: `\\`},
		{s: `"`, esc: `\"`},
	}
)

// escapeMeasurement escapes a measurement.
func escapeMeasurement(in string) string {
	return escape(in, measurementEscapeCodes)
}

// escapeTag escapes a tag key or value.
func escapeTag(in string) string {
	return escape(in, tagEscapeCodes)
}

// escapeString escapes a string field key or value.
func escapeString(in string) string {
	return escape(in, stringEscapeCodes)
}

// escape the string with the given escape sequences.
func escape(in string, codes []escapeSequence) string {
	for _, c := range codes {
		in = strings.Replace(in, c.s, c.esc, -1)
	}
	return in
}

// formatValue formats a value as a string.
func formatValue(v interface{}) (string, error) {
	switch v := v.(type) {
	case float64:
		return strconv.FormatFloat(v, 'g', 6, 64), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'g', 6, 64), nil
	case int64:
		return strconv.FormatInt(v, 10) + "i", nil
	case int32:
		return strconv.FormatInt(int64(v), 10) + "i", nil
	case int:
		return strconv.Itoa(v) + "i", nil
	case string:
		return `"` + escapeString(v) + `"`, nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	default:
		return "", fmt.Errorf("invalid field type: %T", v)
	}
}
