package influxdb

// QueryOption is an option to customize a query.
type QueryOption interface {
	apply(opt *QueryOptions)
}

type queryOptionFunc func(opt *QueryOptions)

func (f queryOptionFunc) apply(opt *QueryOptions) {
	f(opt)
}

// Param sets a bound parameter in the query.
func Param(key string, val interface{}) QueryOption {
	return queryOptionFunc(func(opt *QueryOptions) {
		opt.Params[key] = val
	})
}

// Params sets bound parameters from a map.
func Params(params map[string]interface{}) QueryOption {
	return queryOptionFunc(func(opt *QueryOptions) {
		for key, val := range params {
			opt.Params[key] = val
		}
	})
}
