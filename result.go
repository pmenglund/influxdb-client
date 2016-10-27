package influxdb

type Row []interface{}

type ResultSet interface {
	Columns() []string
	Column(index int) string
	NextRow() (Row, error)
}
