package influxdb

import (
	"io"
	"net/http"
)

// Precision is the requested precision.
type Precision string

const (
	// PrecisionNanosecond represents nanosecond precision.
	PrecisionNanosecond = Precision("n")

	// PrecisionMicrosecond represents microsecond precision.
	PrecisionMicrosecond = Precision("u")

	// PrecisionMillisecond represents millisecond precision.
	PrecisionMillisecond = Precision("ms")

	// PrecisionSecond represents second precision.
	PrecisionSecond = Precision("s")

	// PrecisionMinute represents minute precision.
	PrecisionMinute = Precision("m")

	// PrecisionHour represents hour precision.
	PrecisionHour = Precision("h")
)

func (p Precision) String() string {
	return string(p)
}

// Consistency is the requested consistency of the write.
type Consistency string

const (
	// ConsistencyAll requires all data nodes to acknowledge a write.
	ConsistencyAll = Consistency("all")

	// ConsistencyOne requires at least one data node acknowledge a write.
	ConsistencyOne = Consistency("one")

	// ConsistencyQuorum requires a quorum of data nodes to acknowledge a write.
	ConsistencyQuorum = Consistency("quorum")

	// ConsistencyAny allows for hinted hand off, potentially no write happened yet.
	ConsistencyAny = Consistency("any")
)

func (c Consistency) String() string {
	return string(c)
}

// Client is a client that communicates with an InfluxDB server.
type Client struct {
	// HTTP client used to talk to the InfluxDB HTTP server.
	*http.Client

	// Proto is the protocol to use when issuing client requests. If this is left
	// blank, it defaults to http.
	Proto string

	// Addr is the address to use when issuing client requests. If this is left
	// blank, it defaults to localhost:8086.
	Addr string

	// Path is the default HTTP path to prefix to all requests.
	Path string

	// Database is the default database to use when writing or querying.
	Database string

	// RetentionPolicy is the default database to use when writing or querying.
	RetentionPolicy string
}

// NewClient creates a new client pointed to the parsed hostname.
func NewClient(url string) (*Client, error) {
	proto, addr, path, err := ParseUrl(url)
	if err != nil {
		return nil, err
	}

	return &Client{
		Proto: proto,
		Addr:  addr,
		Path:  path,
	}, nil
}

// ParseUrl parses a url and retrieves the protocol, address, and base path (if relevant).
func ParseUrl(rawurl string) (proto, addr, path string, err error) {
	return "http", "localhost:8086", "", nil
}

// QueryOptions is a set of configuration options for configuring queries.
type QueryOptions struct {
	Database        string
	RetentionPolicy string
	Chunked         bool
	ChunkSize       int
	Pretty          bool
	Format          string
	Async           bool
}

// QueryMeta has meta information about the query returned by the server.
type QueryMeta struct {
	Format string
}

// Query executes a query and parses the results from the stream.
func (c *Client) Query(q string, opt *QueryOptions) (Reader, QueryMeta, error) {
	r, meta, err := c.RawQuery(q, opt)
	if err != nil {
		return nil, meta, err
	}

	reader, err := NewReader(r, meta)
	return reader, meta, err
}

// RawQuery executes a query and returns the raw stream.
func (c *Client) RawQuery(q string, opt *QueryOptions) (io.ReadCloser, QueryMeta, error) {
	return nil, QueryMeta{}, nil
}

// WriteOptions is a set of configuration options for writes.
type WriteOptions struct {
	Database        string
	RetentionPolicy string
	Precision       Precision
	Consistency     Consistency
}

// Write writes a batch of points over the line protocol to the HTTP /write endpoint.
func (c *Client) Write(points []Point, opt *WriteOptions) error {
	return nil
}
