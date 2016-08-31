# InfluxDB Client
[![GoDoc Reference](https://godoc.org/github.com/influxdata/influxdb-client?status.svg)](https://godoc.org/github.com/influxdata/influxdb-client)

An experimental client library for InfluxDB. This client library is
designed to make working with InfluxDB easier and support the full
breadth of features within InfluxDB at the same time.

## Example

```go
client := influxdb.Client{}

influxdb.Discard(client.Query("CREATE DATABASE mydb", nil))
defer influxdb.Discard(client.Query("DROP DATABASE mydb", nil))

now := time.Now().Truncate(time.Second).Add(-99*time.Second)
pts := make([]influxdb.Point, 0, 100)
for i := 0; i < 100; i++ {
	pts = append(pts, influxdb.NewPoint("cpu", influxdb.Value(i), now.Add(i*time.Second)))
}
client.Write(pts...)

reader, _, err := client.Query("SELECT mean(value) FROM cpu")
if err != nil {
	log.Fatal(err)
}
defer reader.Close()

result := influxdb.Result{}
for {
	if err := reader.Read(&result); err != nil {
		log.Fatal(err)
  }

	s := result.Series[0]
	fmt.Printf("name: %v\n", s.Name)
	for _, values := range s.Values {
		for i, column := range s.Columns {
			fmt.Printf("%v: %v\n", column, values[i])
		}
	}
}
```

## Writing Data

Writing data to a database is very simple.

```go
client := influxdb.Client{}
pt := influxdb.NewPoint("cpu", influxdb.Value(2.0), time.Time{})
client.Write(pt)
```

This will write the following line over the line protocol:

```
cpu value=2.0
```

### Using custom fields

The most common field key is `value` and the
`influxdb.Value(interface{})` function exists for this common use case.
If you need to write more than one field, you can create a map of
fields very easily.

```go
client := influxdb.Client{}

fields := influxdb.Fields{
	"value": 2.0,
	"total": 10.0,
}
pt := influxdb.NewPoint("cpu", fields, time.Time{})

client.Write(pt)
```

Any of the following types can be used as a field value. The real type
is in bold and any of the other supported types will be cast to the
bolded type by the client.

* float32, **float64**
* int, int32, **int64**
* **string**
* **bool**

Unsigned integers aren't supported by InfluxDB and they are not
automatically cast so that precision isn't lost.

### Writing a point with tags

A point can be written with tags very easily by passing in a list of
tags.

```go
client := influxdb.Client{}

tags := []influxdb.Tag{
	{Key: "host", Value: "server01"},
  {Key: "region", Value: "useast"},
}
pt := influxdb.NewPointWithTags("cpu", tags, influxdb.Value(2.0), time.Time{})

client.Write(pt)
```

When writing, the tags should be sorted for best write performance. The
API uses a slice instead of a map for keeping tags so the writer does
not have to create a slice and sort the tags itself everytime you write.
For best performance, try to reuse tags for multiple calls.
