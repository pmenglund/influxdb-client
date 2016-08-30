package influxdb

// HTTPWriter writes points in line protocol to the HTTP /write endpoint.
type HTTPWriter struct {
	client *Client
	opt    *WriteOptions
}

// NewHTTPWriter creates a new HTTPWriter.
func NewHTTPWriter(client *Client, opt *WriteOptions) Writer {
	return nil
}

func (w *HTTPWriter) Write(points ...Point) error {
	return w.client.Write(points, w.opt)
}
