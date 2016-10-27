package influxdb

// DefaultClient is the default InfluxDB client.
var DefaultClient = &Client{}

func Ping() error {
	return DefaultClient.Ping()
}
