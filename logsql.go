package main

import (
	"bytes"
	"fmt"
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
	user string
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

func isLogin(zzzz []byte) (isLogin bool){
	log.Println("isLogin")
	isLogin = len(zzzz) == 23
	if !isLogin{
		return
	}
	for  _, b := range zzzz {
		if b==0{
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
		n, err := src.Read(buffer)
		if err != nil {
			return
		}

		if err != nil {
			log.Println(err.Error())
		}
		user, ok := UserMap[sessionKey]
		if !ok{
			zzzz := buffer[pos1:pos]
			if isLogin(zzzz) {
				user := string(buffer[pos : pos+bytes.IndexByte(buffer[pos:], 0)])
				UserMap[sessionKey] = user
				sqlInfo.user = user
				log.Println(user)
			}
		} else{
			sqlInfo.user = user
		}
		if n >= Bs {
			var verboseStr string
			//log.Printf("%s" , buffer[Bs:])
			//log.Printf("%x" , buffer)
			//log.Printf("%x" , buffer[:Bs])
			switch buffer[Bs-1] {
			case comQuit:
				verboseStr = fmt.Sprintf("From %s To %s; Quit: %s\n", sqlInfo.client, sqlInfo.server, "user quit")
				sqlInfo.sqlType = "Quit"
			case comInitDB:
				verboseStr = fmt.Sprintf("From %s To %s; schema: use %s\n", sqlInfo.client, sqlInfo.server, string(buffer[Bs:n]))
				sqlInfo.sqlType = "Schema"
			case comQuery:
				verboseStr = fmt.Sprintf("From %s To %s; Query: %s\n", sqlInfo.client, sqlInfo.server, string(buffer[Bs:n]))
				sqlInfo.sqlType = "Query"
			//case comFieldList:
			//	verboseStr = log.Printf("From %s To %s; Table columns list: %s\n", sqlInfo.client, sqlInfo.server, string(buffer[Bs:n]))
			//	sqlInfo.sqlType = "Table columns list"
			case comCreateDB:
				verboseStr = fmt.Sprintf("From %s To %s; CreateDB: %s\n", sqlInfo.client, sqlInfo.server, string(buffer[Bs:n]))
				sqlInfo.sqlType = "CreateDB"
			case comDropDB:
				verboseStr = fmt.Sprintf("From %s To %s; DropDB: %s\n", sqlInfo.client, sqlInfo.server, string(buffer[Bs:n]))
				sqlInfo.sqlType = "DropDB"
			case comRefresh:
				verboseStr = fmt.Sprintf("From %s To %s; Refresh: %s\n", sqlInfo.client, sqlInfo.server, string(buffer[Bs:n]))
				sqlInfo.sqlType = "Refresh"
			case comStmtPrepare:
				verboseStr = fmt.Sprintf("From %s To %s; Prepare Query: %s\n", sqlInfo.client, sqlInfo.server, string(buffer[Bs:n]))
				sqlInfo.sqlType = "Prepare Query"
			case comStmtExecute:
				verboseStr = fmt.Sprintf("From %s To %s; Prepare Args: %s\n", sqlInfo.client, sqlInfo.server, string(buffer[Bs:n]))
				sqlInfo.sqlType = "Prepare Args"
			case comProcessKill:
				verboseStr = fmt.Sprintf("From %s To %s; Kill: kill conntion %s\n", sqlInfo.client, sqlInfo.server, string(buffer[Bs:n]))
				sqlInfo.sqlType = "Kill"
			default:
			}

			if Verbose {
				log.Print(verboseStr)
			}

			if strings.EqualFold(sqlInfo.sqlType, "Quit") {
				sqlInfo.sqlString = "user quit"
			} else {
				sqlInfo.sqlString = converToUnixLine(sql_escape(string(buffer[Bs:n])))
			}
			//log.Printf(sqlInfo.client)
			//log.Printf(sqlInfo.server)
			//log.Printf(sqlInfo.sqlType)
			//log.Printf(sqlInfo.sqlString)
			if !strings.EqualFold(sqlInfo.sqlType, "") && Dbh != nil {

				//log.Printf("insertlog")
				insertlog(Dbh, &sqlInfo)
			}

		}

		_, err = dst.Write(buffer[0:n])
		if err != nil {
			return
		}
	}
}
