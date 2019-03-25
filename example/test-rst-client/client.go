package main

import (
	"crypto/rand"
	"net"
)

func main() {
	var err error
	var conn net.Conn
	if conn, err = net.Dial("tcp", "127.0.0.1:9922"); err != nil {
		panic(err)
	}
	buf := make([]byte, 1024)
	rand.Read(buf)
	conn.(*net.TCPConn).SetLinger(0)
	go conn.Write(buf)
}
