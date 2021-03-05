package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"log"
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

func isLogin(zzzz []byte) (isLogin bool) {
	log.Println("isLogin")
	isLogin = len(zzzz) == 23
	if !isLogin {
		return
	}
	for _, b := range zzzz {
		if b == 0 {
			continue
		} else {
			isLogin = false
		}
	}
	return
}

func proxyLog(src, dst *Conn) {
	buffer := make([]byte, Bsize)
	var sqlInfo query
	sqlInfo.client, sqlInfo.cport = ipPortFromNetAddr(src.conn.RemoteAddr().String())
	sqlInfo.server, sqlInfo.sport = ipPortFromNetAddr(dst.conn.RemoteAddr().String())
	sessionKey := fmt.Sprintf("%s:%s->%s:%s", sqlInfo.client, sqlInfo.cport, sqlInfo.server, sqlInfo.sport)
	_, sqlInfo.bindPort = ipPortFromNetAddr(src.conn.LocalAddr().String())
	pos1 := 13
	pos := 36
	for {
		var payload []byte
		n, err := src.Read(buffer)
		if err != nil && err != io.EOF {
			log.Printf("src.Read Error: %s", err.Error())
		}
		_, err = dst.Write(buffer[0:n])
		if err != nil {
			log.Printf("dst.Write Error: %s", err.Error())
		}
		user, ok := UserMap[sessionKey]
		if ok {
			sqlInfo.user = user
		} else {
			zzzz := buffer[pos1:pos]
			if isLogin(zzzz) {
				user := string(buffer[pos : pos+bytes.IndexByte(buffer[pos:], 0)])
				UserMap[sessionKey] = user
				sqlInfo.user = user
				continue
			}
		}
		compressDataSize := buffer[:3]
		cZize := int(binary.LittleEndian.Uint16(compressDataSize))
		if cZize == n-7 {
			header := buffer[:7]
			comprLength := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
			uncompressedLength := int(uint32(header[4]) | uint32(header[5])<<8 | uint32(header[6])<<16)
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
					log.Printf("zlib.NewReader: %s", err.Error())
				}
				data := make([]byte, uncompressedLength)
				lenRead := 0
				for lenRead < uncompressedLength {
					tmp := data[lenRead:]
					n, err := r.Read(tmp)
					lenRead += n
					if err != nil {
						log.Printf("lenRead: %d", lenRead)
						log.Printf("uncompressedLength: %d", uncompressedLength)
						if err == io.EOF {
							if lenRead < uncompressedLength {
								log.Printf("ErrUnexpectedEOF: %s", io.ErrUnexpectedEOF)
							}
						} else {
							log.Printf("not EOF: %s", err.Error())
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
			//log.Printf("%x", args)
			switch  cmd{
			case comQuit:
				sqlInfo.sqlType = "Quit"
			case comInitDB:
				sqlInfo.sqlType = "Schema"
			case comQuery:
				sqlInfo.sqlType = "Query"
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
			}
			if strings.EqualFold(sqlInfo.sqlType, "Quit") {
				sqlInfo.sqlString = "user quit"
			} else {
				sqlInfo.sqlString = converToUnixLine(sql_escape(string(args)))
			}
			//log.Printf(sqlInfo.client)
			//log.Printf(sqlInfo.server)
			//log.Printf(sqlInfo.sqlType)
			//log.Printf(sqlInfo.sqlString)
			if !strings.EqualFold(sqlInfo.sqlType, "") && Dbh != nil {
				insertlog(Dbh, &sqlInfo)
			}
		}
	}
}
