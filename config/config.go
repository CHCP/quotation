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

//InputMessage
type InputMessage struct {
	S string    `json:"s,omitempty"`
	U time.Time `json:"u,omitempty"`
	C float32   `json:"c,omitempty"`
	V int       `json:"v,omitempty"`
}

//OutputMessage
type OutputMessage struct {
	S string    `json:"s,omitempty"`
	U time.Time `json:"u,omitempty"`
	C float32   `json:"c,omitempty"`
	V int       `json:"v,omitempty"`
	O float32   `json:"o,omitempty"`
	H int       `json:"h,omitempty"`
	L float32   `json:"l,omitempty"`
	T string    `json:"t,omitempty"`
}
