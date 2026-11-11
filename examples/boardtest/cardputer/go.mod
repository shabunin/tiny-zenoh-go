module github.com/shabunin/gozenoh/examples/boardtest/cardputer

go 1.26.10

replace github.com/shabunin/gozenoh => ../../..

require (
	github.com/shabunin/gozenoh v0.0.0
	tinygo.org/x/drivers v0.37.0
	tinygo.org/x/espradio v0.5.0
)

require github.com/soypat/lneto v0.4.0 // indirect
