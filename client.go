package influxdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Auth contains the authentication credentials. This only handles user
// authentication within InfluxDB and doesn't handle any advanced
// authentication methods.
type Auth struct {
	Username string
	Password string
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

	// Auth holds the authentication credentials.
	Auth *Auth
}

// NewClient creates a new client pointed to the parsed hostname.
func NewClient(rawurl string) (*Client, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	var auth *Auth
	if u.User != nil {
		auth = &Auth{Username: u.User.Username()}
		if p, ok := u.User.Password(); ok {
			auth.Password = p
		}
	}
	return &Client{
		Proto: u.Scheme,
		Addr:  u.Host,
		Path:  u.Path,
		Auth:  auth,
	}, nil
}

// ServerInfo contains any fields returned by the /ping endpoint.
type ServerInfo struct {
	Version string
}

// Ping sends a ping to the server to verify the server is alive and accepting
// HTTP requests.
func (c *Client) Ping() (ServerInfo, error) {
	u := c.url("/ping")
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return ServerInfo{}, ErrPing{Cause: err}
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return ServerInfo{}, ErrPing{Cause: err}
	} else if resp.StatusCode/100 != 2 {
		return ServerInfo{}, ErrPing{Cause: errors.New("incorrect status code")}
	}
	return ServerInfo{
		Version: resp.Header.Get("X-Influxdb-Version"),
	}, nil
}

// Querier returns a struct that can be used to save query options and execute queries.
func (c *Client) Querier() *Querier {
	return &Querier{c: c}
}

// NewReadonlyQueryRequest creates a new GET HTTP request for the query.
//
// This request will use a GET and can only contain statements that read from
// the database.
func (c *Client) NewReadonlyQueryRequest(q interface{}, opt QueryOptions) (*http.Request, error) {
	return c.newQueryRequest(q, true, opt)
}

// NewQueryRequest creates a new POST HTTP request for the query.
//
// This request will use a POST and can contain both statements that read and
// modify the database.
func (c *Client) NewQueryRequest(q interface{}, opt QueryOptions) (*http.Request, error) {
	return c.newQueryRequest(q, false, opt)
}

// newQueryRequest creates a new HTTP request for the query.
//
// The first parameter is for a query. This can be either a string or an
// io.Reader. If the query is an io.Reader, the query is sent as a file using
// multipart/form-data when readonly is false. The first is more useful for a
// single ad-hoc query, but the second can be better for running large
// multi-command queries.
//
// The second parameter is whether the query is supposed to be a read-only
// query. This determines how we encode the query string. If we use a string,
// the query is encoded in the url parameters so it can be logged on the
// server. If we use an io.Reader, the entire file is read and encoded in the
// body.
func (c *Client) newQueryRequest(q interface{}, readonly bool, opt QueryOptions) (*http.Request, error) {
	values := url.Values{}

	var body io.Reader
	var contentType string

	method := "POST"
	if readonly {
		method = "GET"
	}

	switch q := q.(type) {
	case string:
		values.Set("q", q)
	case io.Reader:
		if readonly {
			in, err := ioutil.ReadAll(q)
			if err != nil {
				return nil, err
			}

			params := url.Values{}
			params.Set("q", string(in))
			body = strings.NewReader(params.Encode())
			contentType = "application/x-www-form-urlencoded"
		} else {
			buf := bytes.NewBuffer(nil)
			writer := multipart.NewWriter(buf)

			// Retrieve the filename if we are reading from a file.
			// The server doesn't actually use this information, but it's nice to include anyway.
			filename := "<stdin>"
			if f, ok := q.(*os.File); ok {
				filename = filepath.Base(f.Name())
			}

			// Create the form file and copy the contents of the io.Reader into it.
			f, err := writer.CreateFormFile("q", filename)
			if err != nil {
				return nil, err
			}
			io.Copy(f, q)
			writer.Close()

			body = buf
			contentType = writer.FormDataContentType()
		}
	default:
		return nil, fmt.Errorf("invalid query type: %T", q)
	}

	if opt.Database != "" {
		values.Set("db", opt.Database)
	}
	if opt.Chunked {
		values.Set("chunked", "true")
		if opt.ChunkSize > 0 {
			values.Set("chunk_size", strconv.Itoa(opt.ChunkSize))
		}
	}
	if opt.Pretty {
		values.Set("pretty", "true")
	}
	if opt.Async {
		values.Set("async", "true")
	}
	if len(opt.Params) > 0 {
		pout, err := json.Marshal(opt.Params)
		if err != nil {
			return nil, err
		}
		values.Set("params", string(pout))
	}
	values.Set("epoch", PrecisionNanosecond.String())

	u := c.url("/query")
	u.RawQuery = values.Encode()

	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.Auth != nil {
		req.SetBasicAuth(c.Auth.Username, c.Auth.Password)
	}

	switch opt.Format {
	case "text/csv", "csv":
		req.Header.Set("Accept", "text/csv")
	case "application/json", "json", "":
		req.Header.Set("Accept", "application/json")
	default:
		return nil, fmt.Errorf("unknown format: %s", opt.Format)
	}
	return req, nil
}

// Select executes a query and parses the results from the stream.
// To specify options, use Querier to create a Querier and set the options on that.
func (c *Client) Select(q interface{}, opts ...QueryOption) (Cursor, error) {
	querier := Querier{c: c}
	return querier.Select(q, opts...)
}

// Execute executes a query and returns if any error occurred.
// To specify options, use Querier to create a Querier and set the options on that.
func (c *Client) Execute(q interface{}, opts ...QueryOption) error {
	querier := Querier{c: c}
	return querier.Execute(q, opts...)
}

func (c *Client) Writer() *Writer {
	return &Writer{c: c}
}

// url constructs a URL object for this client.
func (c *Client) url(path string) url.URL {
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
	return u
}
