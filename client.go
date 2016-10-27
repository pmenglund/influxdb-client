package influxdb

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
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
	http.Client

	// Proto is the protocol to use when issuing client requests. If this is left
	// blank, it defaults to http.
	Proto string

	// Addr is the address to use when issuing client requests. If this is left
	// blank, it defaults to localhost:8086.
	Addr string

	// Path is the default HTTP path to prefix to all requests.
	Path string

	// Username to use when authenticating requests.
	Username string

	// Password to use when authenticating requests.
	Password string

	// Database is the default database to use when writing or querying.
	Database string

	// RetentionPolicy is the default database to use when writing or querying.
	RetentionPolicy string
}

// NewClient creates a new client pointed to the parsed hostname.
func NewClient(rawurl string) (*Client, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	var user, pass string
	if u.User != nil {
		user = u.User.Username()
		if p, ok := u.User.Password(); ok {
			pass = p
		}
	}
	return &Client{
		Proto:    u.Scheme,
		Addr:     u.Host,
		Path:     u.Path,
		Username: user,
		Password: pass,
	}, nil
}

// Ping sends a ping to the server to verify the server is alive and accepting
// HTTP requests.
func (c *Client) Ping() error {
	return nil
}

// QueryOptions is a set of configuration options for configuring queries.
type QueryOptions struct {
	Database  string
	Chunked   bool
	ChunkSize int
	Pretty    bool
	Format    string
	Async     bool
}

// QueryMeta has meta information about the query returned by the server.
type QueryMeta struct {
	Format string
}

// NewQuery creates a new HTTP request for the query.
//
// The first parameter is for a query. This can either be a string or an io.Reader.
// If the query is a string, then the query is sent in the body with the content-type
// application/x-www-form-urlencoded. If the query is an io.Reader, the query is sent
// as a file using multipart/form-data. The first is more useful for a single ad-hoc
// query, but the second can be better for running large multi-command queries.
func (c *Client) NewQuery(method string, q interface{}, opt QueryOptions) (*http.Request, error) {
	values := url.Values{}
	if opt.Database != "" {
		values.Set("db", opt.Database)
	} else if c.Database != "" {
		values.Set("db", c.Database)
	}

	u := c.url("/query")
	u.RawQuery = values.Encode()

	var body io.Reader
	var contentType string
	switch q := q.(type) {
	case string:
		params := url.Values{}
		params.Set("q", q)
		body = strings.NewReader(params.Encode())
		contentType = "application/x-www-form-urlencoded"
	default:
		return nil, errors.New("error")
	}

	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	return req, nil
}

func (c *Client) Querier() *Querier {
	opts := QueryOptions{Chunked: true}
	return &Querier{c: c, Options: opts}
}

// Select executes a query and parses the results from the stream.
// The parameters are the same as for NewQuery.
func (c *Client) Select(q interface{}, opt *QueryOptions) (ReadCloser, error) {
	return NewReader(nil, "")
}

func (c *Client) Execute(q interface{}, opt *QueryOptions) error {
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
	return &http.Request{Method: "POST", URL: &url.URL{}}, nil
}

// Write writes a batch of points over the line protocol to the HTTP /write endpoint.
func (c *Client) Write(points []Point, opt *WriteOptions) error {
	return nil
}

func (c *Client) url(path string) *url.URL {
	u := url.URL{
		Scheme: c.Proto,
		Host:   c.Addr,
		Path:   path,
	}

	if u.Scheme == "" {
		u.Scheme = "http"
	}
	if u.Host == "" {
		u.Host = "127.0.0.1:8086"
	}
	return &u
}
