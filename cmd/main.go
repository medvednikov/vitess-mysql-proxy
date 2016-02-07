package main

import (
	"flag"
	"log"
	"net"
	"time"

	"github.com/medvednikov/vitess-mysql-proxy"
	mysqlserver "github.com/siddontang/go-mysql/server"
	"github.com/youtube/vitess/go/vt/vitessdriver"

	"database/sql"
	"fmt"
)

var vitessdb *sql.DB

func main() {
	vitessServer := flag.String("vitess_server", "localhost:15999", "vitess server")
	keyspace := flag.String("keyspace", "test_keyspace", "vitess keyspace")
	shard := flag.String("shard", "0", "vitess shard")
	flag.Parse()
	initVitess(*vitessServer, *keyspace, *shard)
	startMySQLServer()
}

func initVitess(server, keyspace, shard string) {
	fmt.Println("Connecting to vitess on ", server, "...")
	timeout := 10 * time.Second
	var err error
	// Connect to vtgate
	vitessdb, err = vitessdriver.OpenShard(
		server, keyspace, shard, "master", timeout)
	if err != nil {
		log.Fatal("vitess client error: ", err)
	}
	fmt.Println("Done\n\n")
	//defer vitessdb.Close()
}

func startMySQLServer() {
	fmt.Println("Starting MySQL server...")
	l, err := net.Listen("tcp", "127.0.0.1:4000")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handleTCPConn(c)
	}
}

func handleTCPConn(c net.Conn) {
	// Create a connection with user root and an empty passowrd
	conn, err := mysqlserver.NewConn(c, "root", "", vitessproxy.VitessHandler{vitessdb})
	if err != nil {
		log.Println(err)
	}
	for {
		conn.HandleCommand()
	}
}
