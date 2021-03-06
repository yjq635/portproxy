package main

/*
Run as a tcp port proxy if there are multi datacentors in your
production, Receive the traffic and redirect to real server.

cz-20151119
*/

import (
	"database/sql"
	"flag"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
)

//ignore signal
func waitSignal() {
	var sigChan = make(chan os.Signal, 1)
	signal.Notify(sigChan)
	for sig := range sigChan {
		if sig == syscall.SIGINT || sig == syscall.SIGTERM {
			log.Printf("terminated by signal %v\n", sig)
			os.Exit(0)
		} else {
			log.Printf("received signal: %v, ignore\n", sig)
		}
	}
}

//net timeout
const timeout = time.Second * 2

var Bsize uint
var Verbose bool
var Dbh *sql.DB
var serviceName = "mysql-proxy"
var  yamlPath string

type T struct {
	Dsn string
	Backends [] struct {
		Server string
		Bind   string
	}
}
func init() {
	var projectConf string
	if runtime.GOOS == "windows" {
		projectConf = serviceName + ".yaml"
	} else {
		projectConf = "/etc/woda/modules/" + serviceName + "/" + serviceName + ".yaml"
	}
	flag.StringVar(&yamlPath, "c", projectConf, "yaml path: /etc/woda/xx.yaml")
}


func main() {
	// options
	var bind, backend, logTo string
	var buffer uint
	var daemon bool
	var verbose bool

	flag.StringVar(&bind, "bind", ":8003", "locate ip and port")
	flag.StringVar(&backend, "backend", "192.168.199.224:3306", "backend server ip and port")
	flag.StringVar(&logTo, "logTo", "stdout", "stdout or syslog")
	flag.UintVar(&buffer, "buffer", 4096, "buffer size")
	flag.BoolVar(&daemon, "daemon", false, "run as daemon process")
	flag.BoolVar(&verbose, "verbose", false, "print verbose sql query")
	flag.Parse()
	Bsize = buffer
	Verbose = verbose
/*
	conf_fh, err := get_config(conf)
	if err != nil {
		log.Printf("Can't get config info, skip insert log to mysql...\n")
		log.Printf(err.Error())
	} else {
	    backend_dsn, _ := get_backend_dsn(conf_fh)
	    Dbh, err = dbh(backend_dsn)
    	if err != nil {
	    	log.Printf("Can't get database handle, skip insert log to mysql...\n")
	    }
	    defer Dbh.Close()
    }
 */
	log.SetOutput(os.Stdout)
	m := T{}
	data, err := ioutil.ReadFile(yamlPath)
	if err != nil {
		log.Printf("ioutil.ReadFile, error:%s", err)
		return
	}
	log.Printf("yamlstr :%s", data)
	err = yaml.Unmarshal([]byte(data), &m)
	if err != nil {
		log.Printf("yaml.Unmarshal error:%s",  err)
		return
	}
	log.Printf("yaml:%s", m)
	backend_dsn := m.Dsn
	Dbh, err = dbh(backend_dsn)
	if err != nil {
		log.Printf("Can't get database handle, skip insert log to mysql...\n")
	}
	defer Dbh.Close()
	forword_server_ip :="192.168.10.29"

	for _, backend := range m.Backends {
		server := backend.Server
		bind := backend.Bind
		p := New(bind, server, uint32(buffer))
		s2 := strings.Split(server, ":")
		server_ip := s2[0]
		server_port := s2[1]
		log.Println("portproxy started.")
		log.Printf("iptables -t nat -A PREROUTING -i tun0 -d %s -p tcp -m tcp --dport %s  -j DNAT --to-destination %s%s", server_ip, server_port, forword_server_ip, bind)
		log.Printf("iptables -t nat -A OUTPUT -d %s -p tcp -m tcp --dport %s  -j DNAT --to-destination %s%s", server_ip, server_port, forword_server_ip, bind)
		go p.Start()
	}
	waitSignal()
}
/*
iptables -t nat -A PREROUTING -d 192.168.10.100 -p tcp -m tcp --dport 3306  -j DNAT --to-destination 192.168.10.29:8003
iptables -t nat -A OUTPUT -d 192.168.10.100 -p tcp -m tcp --dport 3306  -j DNAT --to-destination 192.168.10.29:8003
 */