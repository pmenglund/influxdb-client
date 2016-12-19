package influxdb

// DefaultClient is the default InfluxDB client.
var DefaultClient = &Client{}

// Ping sends a ping to the server to verify the server is alive and accepting
// HTTP requests.
func Ping() (ServerInfo, error) {
	return DefaultClient.Ping()
}

// Querier returns a struct that can be used to save query options and execute queries.
func DefaultQuerier() *Querier {
	return DefaultClient.Querier()
}

// Select executes a query and parses the results from the stream.
// To specify options, use Querier to create a Querier and set the options on that.
func Select(q interface{}, opts ...QueryOption) (Cursor, error) {
	return DefaultClient.Select(q, opts...)
}

// Execute executes a query and returns if any error occurred.
// To specify options, use Querier to create a Querier and set the options on that.
func Execute(q interface{}, opts ...QueryOption) error {
	return DefaultClient.Execute(q, opts...)
}
