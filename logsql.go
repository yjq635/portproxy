package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

//read more client-server protocol from http://dev.mysql.com/doc/internals/en/text-protocol.html
const (
	comQuit byte = iota + 1
	comInitDB
	comQuery
	comFieldList
	comCreateDB
	comDropDB
	comRefresh
	comShutdown
	comStatistics
	comProcessInfo
	comConnect
	comProcessKill
	comDebug
	comPing
	comTime
	comDelayedInsert
	comChangeUser
	comBinlogDump
	comTableDump
	comConnectOut
	comRegiserSlave
	comStmtPrepare
	comStmtExecute
	comStmtSendLongData
	comStmtClose
	comStmtReset
	comSetOption
	comStmtFetch
)

type query struct {
	bindPort  int64
	client    string
	cport     int64
	server    string
	sport     int64
	sqlType   string
	sqlString string
	user      string
}

func ipPortFromNetAddr(s string) (ip string, port int64) {
	addrInfo := strings.SplitN(s, ":", 2)
	ip = addrInfo[0]
	port, _ = strconv.ParseInt(addrInfo[1], 10, 64)
	return
}

func converToUnixLine(sql string) string {
	sql = strings.Replace(sql, "\r\n", "\n", -1)
	sql = strings.Replace(sql, "\r", "\n", -1)
	return sql
}

func sql_escape(s string) string {
	var j int = 0
	if len(s) == 0 {
		return ""
	}

	tempStr := s[:]
	desc := make([]byte, len(tempStr)*2)
	for i := 0; i < len(tempStr); i++ {
		flag := false
		var escape byte
		switch tempStr[i] {
		case '\r':
			flag = true
			escape = '\r'
			break
		case '\n':
			flag = true
			escape = '\n'
			break
		case '\\':
			flag = true
			escape = '\\'
			break
		case '\'':
			flag = true
			escape = '\''
			break
		case '"':
			flag = true
			escape = '"'
			break
		case '\032':
			flag = true
			escape = 'Z'
			break
		default:
		}
		if flag {
			desc[j] = '\\'
			desc[j+1] = escape
			j = j + 2
		} else {
			desc[j] = tempStr[i]
			j = j + 1
		}
	}
	return string(desc[0:j])
}


func getLoginUser(buffer []byte) (user string,compress bool, err error) {
	pos1 := 13
	pos := 36
	zzzz := buffer[pos1:pos]
	Log.Infof("auth package:%x", buffer)
	Log.Info("isLogin")
	clientFlag := buffer[4]
	compress = clientFlag&32>0
	isLogin := len(zzzz) == 23
	for _, b := range zzzz {
		if b == 0 {
			continue
		} else {
			isLogin = false
		}
	}
	if !isLogin {
		err = errors.New("第一个包不是auth包")
		return
	}
	//Log.Infof("%x", buffer)
	userEnd := bytes.IndexByte(buffer[pos:], 0)
	// Log.Info(userEnd)
	if userEnd>0{
		user = string(buffer[pos : pos+userEnd])
	}
	return
}
func MinInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}
func ReadN(conn *Conn, n int) ([]byte, error) {
	readBytes := make([]byte, n)
	tmpBytes := make([]byte, n) // todo reset?

	for i := 0; i < n; {
		readN, err := conn.Read(tmpBytes)
		if err != nil {
			return nil, fmt.Errorf("Reading %v+%v: %s", readBytes, tmpBytes, err)
		}
		copy(readBytes[i:], tmpBytes[:MinInt(n-i, readN)])
		i += readN
	}

	if n != len(readBytes) {
		panic(fmt.Sprintf("Expected to read '%d' bytes but got '%d'", n, len(readBytes)))
	}

	return readBytes, nil
}

func ReadPacket(conn *Conn) ([]byte, error) {
	headerBytes, err := ReadN(conn, 4)
	if err != nil {
		return nil, fmt.Errorf("reading header: %s", err)
	}
	length := int(uint32(headerBytes[0]) | uint32(headerBytes[1])<<8 | uint32(headerBytes[2])<<16)
	if length > 0 {
		dataBytes, err := ReadN(conn, length)
		if err != nil {
			return nil, fmt.Errorf("reading data: %s", err)
		}
		rBytes := append(headerBytes, dataBytes...)
		return rBytes, nil
	}

	return nil, fmt.Errorf("unexpected length")
}

func readOnePacket(conn *Conn, compress bool) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, err
	}
	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if compress{
		length +=3
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, err
	}
	rBytes := append(header[:], data...)
	return rBytes, nil
}

func proxyLog(src, dst *Conn) {

	var sqlInfo query
	sqlInfo.client, sqlInfo.cport = ipPortFromNetAddr(src.conn.RemoteAddr().String())
	sqlInfo.server, sqlInfo.sport = ipPortFromNetAddr(dst.conn.RemoteAddr().String())
	_, sqlInfo.bindPort = ipPortFromNetAddr(src.conn.LocalAddr().String())

	//buffer := make([]byte, Bsize)
	//n, err := src.Read(buffer)
	buffer,err := readOnePacket(src, false)
	if err != nil{
		Log.Infof("src.Read auth Error: %s", err.Error())
	}
	_, err = dst.Write(buffer)
	if err != nil{
		Log.Infof("src.Write auth Error: %s", err.Error())
	}
	user,compress, err := getLoginUser(buffer)
	sqlInfo.user = user
	if err != nil{
		Log.Info(err.Error())
		return
	}

	for {
		var payload []byte
		buffer,err := readOnePacket(src, compress)
		if err != nil {
			if err != io.EOF{
				Log.Infof("src.Read Error: %s", err.Error())
			}
			return
		}
		_, err = dst.Write(buffer)
		if err != nil {
			Log.Infof("dst.Write Error: %s", err.Error())
			return
		}
		n := len(buffer)
		compressDataSize := buffer[:3]
		cZize := int(binary.LittleEndian.Uint16(compressDataSize))
		Log.Infof("n:%d", n)
		Log.Infof("cZize:%d", cZize)
		if cZize == n-7 {
			header := buffer[:7]
			comprLength := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
			Log.Infof("comprLength:%d", comprLength)
			uncompressedLength := int(uint32(header[4]) | uint32(header[5])<<8 | uint32(header[6])<<16)
			Log.Infof("uncompressedLength:%d", uncompressedLength)
			// compressionSequence := uint8(header[3])
			comprData := buffer[7 : 7+comprLength]
			if uncompressedLength == 0 {
				payload = buffer[7:n]
			} else {
				var b bytes.Buffer
				b.Write(comprData)
				r, err := zlib.NewReader(&b)
				if r != nil {
					defer r.Close()
				}
				if err != nil {
					Log.Infof("zlib.NewReader: %s", err.Error())
				}
				data := make([]byte, uncompressedLength)
				lenRead := 0
				for lenRead < uncompressedLength {
					tmp := data[lenRead:]
					n, err := r.Read(tmp)
					lenRead += n
					if err != nil {
						if err == io.EOF {
							if lenRead < uncompressedLength {
								Log.Infof("lenRead: %d, uncompressedLength: %d,ErrUnexpectedEOF: %s", lenRead, uncompressedLength, io.ErrUnexpectedEOF)
							}
						} else {
							Log.Infof("not EOF: %s", err.Error())
						}
					}
				}
				payload = append(payload, data...)
			}
		} else {
			payload = buffer[:n]
		}

		if len(payload)>4 {
			dataBody :=payload[4:]
			cmd := dataBody[0]
			args := dataBody[1:]
			switch  cmd{
			case comQuit:
				sqlInfo.sqlType = "Quit"
			case comInitDB:
				sqlInfo.sqlType = "Schema"
			case comQuery:
				sqlInfo.sqlType = "Query"
			//case comPing:
			//	continue
			//case comFieldList:
			//	sqlInfo.sqlType = "Table columns list"
			case comCreateDB:
				sqlInfo.sqlType = "CreateDB"
			case comDropDB:
				sqlInfo.sqlType = "DropDB"
			case comRefresh:
				sqlInfo.sqlType = "Refresh"
			case comStmtPrepare:
				sqlInfo.sqlType = "Prepare Query"
			case comStmtExecute:
				sqlInfo.sqlType = "Prepare Args"
			case comProcessKill:
				sqlInfo.sqlType = "Kill"
			default:
				Log.Infof("%s not handler data:%x", src.conn.RemoteAddr().String(),buffer[:n])
				continue
			}
			if strings.EqualFold(sqlInfo.sqlType, "Quit") {
				sqlInfo.sqlString = "user quit"
			} else {
				sqlInfo.sqlString = converToUnixLine(sql_escape(string(args)))
			}
			if sqlInfo.sqlString == ""{
				Log.Infof("%x", buffer[:n])
			}
			if !strings.EqualFold(sqlInfo.sqlType, "") && Dbh != nil {
				insertlog(Dbh, &sqlInfo)
			}
		}
	}
}
