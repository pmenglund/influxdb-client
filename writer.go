package influxdb

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// WriteOptions is a set of configuration options for configuring writers.
type WriteOptions struct {
	Database        string
	RetentionPolicy string
	Consistency     Consistency
	Precision       Precision
	Protocol        Protocol
}

// Clone creates a copy of the WriteOptions.
func (opt *WriteOptions) Clone() WriteOptions {
	return *opt
}

// Writer holds onto write options and acts as a convenience method for performing writes.
type Writer struct {
	c *Client
	WriteOptions
}

// Write writes the bytes to the server. The data should be in the line
// protocol format specified in the WriteOptions attached to this writer so the
// server understands the format. Each call to Write will make a single HTTP
// write request.
func (w *Writer) Write(data []byte) (n int, err error) {
	if len(data) == 0 {
		return 0, nil
	}

	values := url.Values{}
	if w.Database != "" {
		values.Set("db", w.Database)
	}
	if w.RetentionPolicy != "" {
		values.Set("rp", w.RetentionPolicy)
	}
	if consistency := w.Consistency.String(); consistency != "" {
		values.Set("consistency", consistency)
	}
	if precision := w.Precision.String(); precision != "" {
		values.Set("precision", precision)
	}

	u := w.c.url("/write")
	u.RawQuery = values.Encode()

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(data))
	if err != nil {
		return 0, err
	}

	p := w.Protocol
	if p == nil {
		p = DefaultWriteProtocol
	}
	req.Header.Set("Content-Type", p.ContentType())
	if w.c.Auth != nil {
		req.SetBasicAuth(w.c.Auth.Username, w.c.Auth.Password)
	}

	resp, err := w.c.Do(req)
	if err != nil {
		return 0, err
	}

	switch resp.StatusCode / 100 {
	case 2:
		return len(data), nil
	case 4:
		// This is a client error. Read the error message to learn what type of
		// error this is.
		err := ReadError(resp)
		if strings.HasPrefix(err.Error(), "partial write:") {
			// So we DID write, but it was a partial write. Wrap the error message.
			return len(data), ErrPartialWrite{Err: err.Error()}
		}
		return 0, err
	default:
		// The server should never actually return anything other than the
		// above, but catch any weird status codes that might get thrown by a
		// proxy or something.
		return 0, ReadError(resp)
	}
}

// ReadFrom will read data from another io.Reader. This is used so io.Copy can
// be supported. In the future, this function may become protocol aware. For
// now, it reads the entire output into a buffer and writes from that buffer.
func (w *Writer) ReadFrom(r io.Reader) (n int, err error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}
	return w.Write(data)
}

// WritePoint will encode a single point in the protocol format and write it to
// the server. While useful for writing a single point, this method is very
// inefficient when writing many points.
func (w *Writer) WritePoint(pt Point) (n int, err error) {
	p := w.Protocol
	if p == nil {
		p = DefaultWriteProtocol
	}
	opts := EncodeOptions{Precision: w.Precision}

	var buf bytes.Buffer
	if err := p.Encode(&buf, &pt, opts); err != nil {
		return 0, err
	}
	return w.Write(buf.Bytes())
}

// WriteBatch will encode a batch of points in the protocol format and write it
// to the server. It makes no attempt to split the number of points in the batch.
func (w *Writer) WriteBatch(pts []Point) (n int, err error) {
	p := w.Protocol
	if p == nil {
		p = DefaultWriteProtocol
	}
	opts := EncodeOptions{Precision: w.Precision}

	var buf bytes.Buffer
	for _, pt := range pts {
		if err := p.Encode(&buf, &pt, opts); err != nil {
			return 0, err
		}
	}
	return w.Write(buf.Bytes())
}
