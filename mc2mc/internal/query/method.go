package query

type Method uint8

const (
	MERGE Method = iota
	APPEND
	REPLACE
)
