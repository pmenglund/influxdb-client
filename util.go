package influxdb

import "io"

// EachResult iterates over every ResultSet in the Cursor.
func EachResult(cur Cursor, fn func(ResultSet) error) error {
	for {
		result, err := cur.NextSet()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if err := fn(result); err != nil {
			if err == ErrStop {
				return nil
			}
			return err
		}
	}
}

// EachSeries iterates over every Series in the ResultSet.
func EachSeries(result ResultSet, fn func(Series) error) error {
	for {
		series, err := result.NextSeries()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if err := fn(series); err != nil {
			if err == ErrStop {
				return nil
			}
			return err
		}
	}
}

// EachRow iterates over every Row in the Series.
func EachRow(series Series, fn func(Row) error) error {
	for {
		row, err := series.NextRow()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if err := fn(row); err != nil {
			if err == ErrStop {
				return nil
			}
			return err
		}
	}
}
