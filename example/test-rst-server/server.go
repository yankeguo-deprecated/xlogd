package main

import (
	"log"
	"net"
)

func main() {
	var err error
	var l net.Listener
	if l, err = net.Listen("tcp", "127.0.0.1:9922"); err != nil {
		panic(err)
	}

	var conn net.Conn
	for {
		if conn, err = l.Accept(); err != nil {
			panic(err)
		}
		go handle(conn)
	}
}

func handle(c net.Conn) {
	log.Println("connected")
	defer c.Close()
	defer log.Println("closed")

	var err error
	var l int
	buf := make([]byte, 1024)
	for {
		if l, err = c.Read(buf); err != nil {
			log.Println("error:", err)
			return
		}
		log.Println("read, l:", l)
	}
}
