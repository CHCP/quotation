/*
Function  : config.go
Author	  : Gordon Wang
Created At: 2020.11.14
*/

package config

import "time"

//Kafka parameters
var (
	Brokers  = "localhost:9092"
	Topic    = "testtopic"
	Group    = ""
	Interval = 100
)

//InputOHLC
type InputOHLC struct {
	S string    `json:"s,omitempty"`
	U time.Time `json:"u,omitempty"`
	C float32   `json:"c,omitempty"`
	V int       `json:"v,omitempty"`
}

//OutputOHLC
type OutputOHLC struct {
	S string    `json:"s,omitempty"`
	U time.Time `json:"u,omitempty"`
	C float32   `json:"c,omitempty"`
	V int       `json:"v,omitempty"`
	O float32   `json:"o,omitempty"`
	H float32   `json:"h,omitempty"`
	L float32   `json:"l,omitempty"`
	T string    `json:"t,omitempty"`
}

type OHLC struct {
	M1  OutputOHLC
	M5  OutputOHLC
	M15 OutputOHLC
	M30 OutputOHLC
	H1  OutputOHLC
	H2  OutputOHLC
	H4  OutputOHLC
	D1  OutputOHLC
	W1  OutputOHLC
	MN  OutputOHLC
}
