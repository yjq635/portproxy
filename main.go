package main

/*
Run as a tcp port proxy if there are multi datacentors in your
production, Receive the traffic and redirect to real server.

cz-20151119
*/

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"os/signal"
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
var UserMap = make(map[string]string)

func main() {
	// options
	var bind, backend, logTo string
	var buffer uint
	var daemon bool
	var verbose bool
	var conf string

	flag.StringVar(&bind, "bind", ":8003", "locate ip and port")
	flag.StringVar(&backend, "backend", "192.168.199.224:3306", "backend server ip and port")
	flag.StringVar(&logTo, "logTo", "stdout", "stdout or syslog")
	flag.UintVar(&buffer, "buffer", 4096, "buffer size")
	flag.BoolVar(&daemon, "daemon", false, "run as daemon process")
	flag.BoolVar(&verbose, "verbose", false, "print verbose sql query")
	flag.StringVar(&conf, "conf", "conf.cnf", "config file to verify database and record sql query")
	flag.Parse()
	Bsize = buffer
	Verbose = verbose

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

	log.SetOutput(os.Stdout)

	p := New(bind, backend, uint32(buffer))
	log.Println("portproxy started.")
	go p.Start()
	waitSignal()
}
/*
iptables -t nat -A PREROUTING -d 192.168.10.100 -p tcp -m tcp --dport 3306  -j DNAT --to-destination 192.168.10.29:8003
iptables -t nat -A OUTPUT -d 192.168.10.100 -p tcp -m tcp --dport 3306  -j DNAT --to-destination 192.168.10.29:8003
 */