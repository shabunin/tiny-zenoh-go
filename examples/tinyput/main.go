package main

import "github.com/shabunin/gozenoh/zenoh"

func main() {
	s, err := zenoh.Open(zenoh.Config{Connect: []string{"tcp/127.0.0.1:7447"}})
	if err != nil {
		println(err.Error())
		return
	}
	_ = s.Put("tinygo/hello", []byte("hi"))
	s.Close()
}
