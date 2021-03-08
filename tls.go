package main

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
)

type ReadableConn struct {
	Conn net.Conn
	Tag  string
}

func NewReadableConn(conn net.Conn, tag string) *ReadableConn {
	return &ReadableConn{conn, tag}
}

func (c *ReadableConn) ReadN(n int) ([]byte, error) {
	readBytes := make([]byte, n)
	tmpBytes := make([]byte, n) // todo reset?

	for i := 0; i < n; {
		readN, err := c.Conn.Read(tmpBytes)
		if err != nil {
			return nil, fmt.Errorf("Reading %v+%v: %s", readBytes, tmpBytes, err)
		}
		copy(readBytes[i:], tmpBytes[:c.minInt(n-i, readN)])
		i += readN
	}

	if n != len(readBytes) {
		panic(fmt.Sprintf("Expected to read '%d' bytes but got '%d'", n, len(readBytes)))
	}

	return readBytes, nil
}

func (ReadableConn) minInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func readPacket(conn *ReadableConn) ([]byte, error) {
	headerBytes, err := conn.ReadN(4)
	if err != nil {
		return nil, fmt.Errorf("reading header: %s", err)
	}

	if headerBytes[0] > 0 && headerBytes[1] == 0 && headerBytes[2] == 0 {
		dataBytes, err := conn.ReadN(int(headerBytes[0]))
		if err != nil {
			return nil, fmt.Errorf("reading data: %s", err)
		}

		bytes := append(headerBytes, dataBytes...)

		return bytes, nil
	}

	return nil, fmt.Errorf("unexpected length")
}

func  writePacket(conn *ReadableConn, bytes []byte) error {
	_, err := conn.Conn.Write(bytes)
	if err != nil {
		return fmt.Errorf("writing data: %s\n", err)
	}
	return nil
}
func clearBit(n int, pos uint) int {
	mask := ^(1 << pos)
	n &= mask
	return n
}

func connectServerAndClient(rawClientConn net.Conn, rawServerConn net.Conn) (*ReadableConn, *ReadableConn, error) {
	clientConn := NewReadableConn(rawClientConn, "client")
	serverConn := NewReadableConn(rawServerConn, "server")

	serverHandshakeBytes, err := readPacket(serverConn)
	if err != nil {
		Log.Errorf("failed reading handshake from server: %s\n", err)
		return serverConn, clientConn, fmt.Errorf("failed reading handshake from server: %s\n", err)
	}

	err = writePacket(clientConn, serverHandshakeBytes)
	if err != nil {
		Log.Errorf("failed forwarding handshake to client: %s\n", err)
		return serverConn, clientConn, fmt.Errorf("failed forwarding handshake to client: %s\n", err)
	}

	clientHandshakeBytes, err := readPacket(clientConn)
	if err != nil {
		Log.Errorf("failed reading handshake from client: %s\n", err)
		return serverConn, clientConn, fmt.Errorf("failed reading handshake from client: %s\n", err)
	}

	// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
	const tlsInitPktLen = 32
	wantsTLS := clientHandshakeBytes[0] == tlsInitPktLen
	serverCertPath := "server-cert.pem"
	serverKeyPath := "server-key.pem"

	serverCert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
	if err != nil{
		Log.Errorf("%s", err)
	}
	tlsConfig := tls.Config{Certificates: []tls.Certificate{serverCert}}
	tlsConfig.Rand = rand.Reader

	if wantsTLS {
		clientConn = NewReadableConn(tls.Server(clientConn.Conn, &tlsConfig), "client-tls")

		// do not send initial tls pkt to server
		var err error

		clientHandshakeBytes, err = readPacket(clientConn)
		if err != nil {
			Log.Errorf("failed reading handshake 2 from client: %s\n", err)
			return serverConn, clientConn, fmt.Errorf("failed reading handshake 2 from client: %s\n", err)
		}

		// adjust seq to 1 from 2 (server did not see init tls packet)
		clientHandshakeBytes[3] = 0x1
		// disable clientSSL flag in handshake
		clientHandshakeBytes[5] = byte(clearBit(int(clientHandshakeBytes[5]), 3))
	}

	err = writePacket(serverConn, clientHandshakeBytes)
	if err != nil {
		return serverConn, clientConn, fmt.Errorf("failed forwarding handshake to server: %s\n", err)
	}

	authRespBytes, err := readPacket(serverConn)
	if err != nil {
		return serverConn, clientConn, fmt.Errorf("failed reading auth resp from server: %s\n", err)
	}

	if wantsTLS {
		authRespBytes[3] = 0x3
	}

	err = writePacket(clientConn, authRespBytes)
	if err != nil {
		return serverConn, clientConn, fmt.Errorf("failed forwarding auth resp to client: %s\n", err)
	}

	return serverConn, clientConn, nil
}

type ConnCopier struct{}

func (c ConnCopier) SrcToDstCopy(dstConn net.Conn, srcConn net.Conn, wg *sync.WaitGroup) {
	_, err := io.Copy(dstConn, srcConn)
	if err != nil {
		fmt.Printf("conn copier: Failed to copy src->dst conn: %s\n", err)
	}

	fmt.Printf("copy finished src->dst\n")

	wg.Done()
}

func (c ConnCopier) DstToSrcCopy(dstConn net.Conn, srcConn net.Conn, wg *sync.WaitGroup) {
	_, err := io.Copy(srcConn, dstConn)
	if err != nil {
		fmt.Printf("conn copier: Failed to copy dst->src conn: %s\n", err)
	}

	fmt.Printf("copy finished dst->src\n")

	wg.Done()
}

func Start(src,dst *Conn){
	serverConn, clientConn, err := connectServerAndClient(src.conn, dst.conn)
	if err != nil{
		Log.Error(err.Error())
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go ConnCopier{}.DstToSrcCopy(serverConn.Conn, clientConn.Conn, &wg)
	go ConnCopier{}.SrcToDstCopy(serverConn.Conn, clientConn.Conn, &wg)
	wg.Wait()
	err = serverConn.Conn.Close()
	err = clientConn.Conn.Close()
}