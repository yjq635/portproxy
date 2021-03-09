package main

import (
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

type Proxy struct {
	bind, backend *net.TCPAddr
	sessionsCount int32
	pool          *recycler
}

func New(bind, backend string, size uint32) *Proxy {
	a1, err := net.ResolveTCPAddr("tcp", bind)
	if err != nil {
		log.Fatalln("resolve bind error:", err)
	}

	a2, err := net.ResolveTCPAddr("tcp", backend)
	if err != nil {
		log.Fatalln("resolve backend error:", err)
	}

	return &Proxy{
		bind:          a1,
		backend:       a2,
		sessionsCount: 0,
		pool:          NewRecycler(size),
	}
}

func (t *Proxy) pipe(dst, src *Conn, c chan int64, tag string) {
	defer func() {
		dst.CloseWrite()
		dst.CloseRead()
	}()
	if strings.EqualFold(tag, "send") {
		proxyLog(src, dst)
		c <- 0
	} else {
		buf := make([]byte, Bsize)
		//n, err := io.CopyBuffer(dst, src, buffer)
		var written int64
		var err error
		nr, er := src.Read(buf)
		if er != nil{
			Log.Errorf("%s", er)
		}
		Log.Infof("handshake package:%x", buf[0:nr])
		//Log.Infof("%d", buf[25:27])
		//flags := uint32(binary.LittleEndian.Uint16(buf[25:27]))
		//Log.Info(flags)
		//Log.Info(flags&ClientSSL)
		buf[26] = 247
		//Log.Infof("%d", buf[30:32])
		//buf[30] = 15
		//buf[31] = 160
		Log.Infof("changed handshake package:%x", buf[0:nr])
		_, ew := dst.Write(buf[0:nr])

		if ew != nil{
			Log.Errorf("%s", ew)
		}
		for {
			nr, er := src.Read(buf)
			if nr > 0 {
				//Log.Infof("%x", buf[:nr])
				nw, ew := dst.Write(buf[0:nr])
				if nw > 0 {
					written += int64(nw)
				}
				if ew != nil {
					err = ew
					break
				}
				if nr != nw {
					err = io.ErrShortWrite
					break
				}
			}
			if er != nil {
				if er != io.EOF {
					err = er
				}
				break
			}
		}
		if err != nil {
			log.Print(err)
		}
		c <- written
	}
}

func (t *Proxy) transport(conn net.Conn) {
	start := time.Now()
	conn2, err := net.DialTCP("tcp", nil, t.backend)
	if err != nil {
		log.Print(err)
		return
	}
	connectTime := time.Now().Sub(start)
	Log.Infof("proxy: %s ==> %s", conn2.LocalAddr().String(),
		conn2.RemoteAddr().String())
	start = time.Now()
	readChan := make(chan int64)
	writeChan := make(chan int64)
	var readBytes, writeBytes int64

	atomic.AddInt32(&t.sessionsCount, 1)
	var bindConn, backendConn *Conn
	bindConn = NewConn(conn, t.pool)
	backendConn = NewConn(conn2, t.pool)

	go t.pipe(backendConn, bindConn, writeChan, "send")
	go t.pipe(bindConn, backendConn, readChan, "receive")

	readBytes = <-readChan
	writeBytes = <-writeChan
	transferTime := time.Now().Sub(start)
	log.Printf("r: %d w:%d ct:%.3f t:%.3f [#%d]", readBytes, writeBytes,
		connectTime.Seconds(), transferTime.Seconds(), t.sessionsCount)
	atomic.AddInt32(&t.sessionsCount, -1)
}

func (t *Proxy) Start() {
	ln, err := net.ListenTCP("tcp", t.bind)
	if err != nil {
		log.Fatal(err)
	}

	defer ln.Close()
	for {
		conn, err := ln.AcceptTCP()
		if err != nil {
			log.Println("accept:", err)
			continue
		}
		Log.Infof("client: %s ==> %s", conn.RemoteAddr().String(),
			conn.LocalAddr().String())
		go t.transport(conn)
	}
}
