package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
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


func getLoginUser(buffer []byte) (user string, err error) {
	pos1 := 13
	pos := 36
	zzzz := buffer[pos1:pos]
	Log.Info("isLogin")
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
	user = string(buffer[pos : pos+bytes.IndexByte(buffer[pos:], 0)])
	return
}

func proxyLog(src, dst *Conn) {
	buffer := make([]byte, Bsize)
	var sqlInfo query
	sqlInfo.client, sqlInfo.cport = ipPortFromNetAddr(src.conn.RemoteAddr().String())
	sqlInfo.server, sqlInfo.sport = ipPortFromNetAddr(dst.conn.RemoteAddr().String())
	_, sqlInfo.bindPort = ipPortFromNetAddr(src.conn.LocalAddr().String())

	n, err := src.Read(buffer)
	if err != nil{
		Log.Infof("src.Read auth Error: %s", err.Error())
	}
	_, err = dst.Write(buffer[0:n])
	if err != nil{
		Log.Infof("src.Write auth Error: %s", err.Error())
	}
	sqlInfo.user, err = getLoginUser(buffer[:n])
	if err != nil{
		Log.Info(err.Error())
		return
	}

	for {
		var payload []byte
		n, err := src.Read(buffer)
		if err != nil {
			if err != io.EOF{
				Log.Infof("src.Read Error: %s", err.Error())
			}
			return
		}
		if n< 5{
			continue
		}
		_, err = dst.Write(buffer[0:n])
		if err != nil {
			Log.Infof("dst.Write Error: %s", err.Error())
			return
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
				Log.Infof("not handler data:%x", buffer[:n])
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
