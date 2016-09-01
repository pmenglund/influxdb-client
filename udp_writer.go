package influxdb

import "net"

// UDPWriter writes points in line protocol to the UDP protocol. Points written
// over UDP may be dropped when the connection is unreliable or is
// oversaturated. Use the HTTPWriter if you need reliable transportation of
// metrics.
type UDPWriter struct {
	Conn net.Conn
}

// NewUDPWriter creates a new UDPWriter.
func NewUDPWriter(addr string) (WriteCloser, error) {
	return &UDPWriter{}, nil
}

// Write writes points to the UDP endpoint. Points written over UDP may be
// dropped when the connection is unreliable or is oversaturated. Use the
// HTTPWriter if you need reliable transportation of metrics.
func (w *UDPWriter) Write(points ...Point) error {
	return nil
}

// Close closes the UDP connection.
func (w *UDPWriter) Close() error {
	return nil
}
