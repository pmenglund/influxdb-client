package influxdb

type Cursor interface {
	NextSet() (ResultSet, error)
	ResultSet
}
