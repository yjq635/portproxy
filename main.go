package main

/*
Run as a tcp port proxy if there are multi datacentors in your
production, Receive the traffic and redirect to real server.

cz-20151119
*/

import (
	"database/sql"
	"flag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

func initLog() *zap.SugaredLogger {


	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		CallerKey:      "caller",
		NameKey:        "logger",
		MessageKey:     "msg",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,  // 小写编码器
		EncodeTime:     zapcore.ISO8601TimeEncoder,     // ISO8601 UTC 时间格式
		EncodeDuration: zapcore.SecondsDurationEncoder, //
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// 设置日志级别
	atomicLevel := zap.NewAtomicLevel()
	atomicLevel.SetLevel(zap.InfoLevel)
/*
   hook := lumberjack.Logger{
   		Filename:   "./logs/package.log", // 日志文件路径
   		MaxSize:    10,                   // 每个日志文件保存的最大尺寸 单位：M
   		MaxBackups: 5,                    // 日志文件最多保存多少个备份
   		MaxAge:     7,                    // 文件最多保存多少天
   		Compress:   true,                 // 是否压缩, 压缩后1M约占20Kb
   	}
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),                                        // 编码器配置
		zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(&hook)), // 打印到控制台和文件
		atomicLevel, // 日志级别
	)
	logger := zap.New(core)

 */

	config := zap.Config{
		Level:       atomicLevel,
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "console",
		EncoderConfig:    encoderConfig,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	logger,_ := config.Build()


	sugar := logger.Sugar()

	logger.Info("log 初始化成功")
	return sugar
}

//ignore signal
func waitSignal() {
	var sigChan = make(chan os.Signal, 1)
	signal.Notify(sigChan)
	for sig := range sigChan {
		if sig == syscall.SIGINT || sig == syscall.SIGTERM {
			Log.Infof("terminated by signal %v\n", sig)
			os.Exit(0)
		} else {
			Log.Infof("received signal: %v, ignore\n", sig)
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
var Log = initLog()

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
const (
	ClientLongPassword uint32 = 1 << iota
	ClientFoundRows
	ClientLongFlag
	ClientConnectWithDB
	ClientNoSchema
	ClientCompress
	ClientODBC
	ClientLocalFiles
	ClientIgnoreSpace
	ClientProtocol41
	ClientInteractive
	ClientSSL
	ClientIgnoreSigpipe
	ClientTransactions
	ClientReserved
	ClientSecureConnection
	ClientMultiStatements
	ClientMultiResults
	ClientPSMultiResults
	ClientPluginAuth
	ClientConnectAtts
	ClientPluginAuthLenencClientData
)
func init1(){

	var capabilityFlags uint32 = 512 | 8 | 524288
	Log.Info(ClientPluginAuth)
	Log.Info(ClientSSL)
	const defaultCapability = ClientLongPassword | ClientLongFlag |
		ClientConnectWithDB | ClientProtocol41 |
		ClientTransactions | ClientSecureConnection | ClientFoundRows |
		ClientMultiStatements | ClientMultiResults | ClientLocalFiles |
		ClientConnectAtts | ClientPluginAuth | ClientInteractive
	Log.Info(capabilityFlags)
	Log.Infof("defaultCapability: %d",defaultCapability)
	capability := defaultCapability
	capability |= ClientSSL
	Log.Infof("|ClientSSL: %d",capability)
	capability &= ClientSSL
	Log.Infof("&ClientSSL: %d",capability)
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
	log.SetOutput(os.Stdout)
	m := T{}
	data, err := ioutil.ReadFile(yamlPath)
	if err != nil {
		Log.Infof("ioutil.ReadFile, error:%s", err)
		return
	}
	err = yaml.Unmarshal([]byte(data), &m)
	if err != nil {
		Log.Infof("yaml.Unmarshal error:%s",  err)
		return
	}
	backend_dsn := m.Dsn
	Dbh, err = dbh(backend_dsn)
	if err != nil {
		Log.Infof("Can't get database handle, skip insert log to mysql...\n")
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
		Log.Info("portproxy started.")
		Log.Infof("iptables -t nat -A PREROUTING -i tun0 -d %s -p tcp -m tcp --dport %s  -j DNAT --to-destination %s%s", server_ip, server_port, forword_server_ip, bind)
		Log.Infof("iptables -t nat -A OUTPUT -d %s -p tcp -m tcp --dport %s  -j DNAT --to-destination %s%s", server_ip, server_port, forword_server_ip, bind)
		go p.Start()
	}
	waitSignal()
}
/*
iptables -t nat -A PREROUTING -d 192.168.10.100 -p tcp -m tcp --dport 3306  -j DNAT --to-destination 192.168.10.29:8003
iptables -t nat -A OUTPUT -d 192.168.10.100 -p tcp -m tcp --dport 3306  -j DNAT --to-destination 192.168.10.29:8003
 */