package influxdb

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
