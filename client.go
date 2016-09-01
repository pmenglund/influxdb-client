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
	Client *http.Client

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

// Do sends an HTTP request using the configured Client or http.DefaultClient.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req)
}

// Ping sends a ping to the server to verify the server is alive and accepting
// HTTP requests.
func (c *Client) Ping() error {
	return nil
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

// NewQuery creates a new HTTP request for the query.
func (c *Client) NewQuery(q string, opt *QueryOptions) (*http.Request, error) {
	return nil, nil
}

// Select executes a query and parses the results from the stream.
func (c *Client) Select(q string, opt *QueryOptions) (Reader, error) {
	r, meta, err := c.SelectRaw(q, opt)
	if err != nil {
		return nil, meta, err
	}

	reader, err := NewReader(r, meta)
	return reader, meta, err
}

// Execute executes a query, discards the results, and returns any error that may have happened.
// If the error happened as a result of a statement failing for some reason, the error is wrapped
// in a ResultError.
func (c *Client) Execute(q string, opt *QueryOptions) error {
	return nil
}

// WriteOptions is a set of configuration options for writes.
type WriteOptions struct {
	Database        string
	RetentionPolicy string
	Precision       Precision
	Consistency     Consistency
}

// NewWrite creates a new HTTP request for the query.
func (c *Client) NewWrite(r io.Reader, opt *WriteOptions) (*http.Request, error) {
	return nil, nil
}

// Write writes a batch of points over the line protocol to the HTTP /write endpoint.
func (c *Client) Write(points []Point, opt *WriteOptions) error {
	return nil
}
