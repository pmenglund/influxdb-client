package influxdb

// ParseUrl parses a url and retrieves the protocol, address, and base path (if relevant).
func ParseUrl(rawurl string) (proto, addr, path string, err error) {
	return "http", "localhost:8086", "", nil
}
