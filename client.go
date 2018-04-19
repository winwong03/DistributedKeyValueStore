/*
Example client for the Regina and The Plastics KV Project
For use in Project 2 of UBC CPSC 416 2017W2

Usage:

$ go run client.go [server ip:port]
*/

package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"

	"./dkvlib"
)

var (
	errLog *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
)

var (
	server              *rpc.Client
	coordinator         *rpc.Client
	localAddr           net.Addr
	coordinatorNodeAddr net.Addr
	nodesAddrs          []net.Addr
	connected           bool
)

// Node - a node of the network
type Node struct {
	ID            string
	IsCoordinator bool
	Address       net.Addr
}

// ConnectServer creates client connection with server
func ConnectServer(serverAddr string) {
	// Look up local addr for the client
	var localAddrStr string
	localHostName, _ := os.Hostname()
	listOfAddr, _ := net.LookupIP(localHostName)
	for _, addr := range listOfAddr {
		if ok := addr.To4(); ok != nil {
			localAddrStr = ok.String()
		}
	}
	localAddrStr = fmt.Sprintf("%s%s", localAddrStr, ":0")
	ln, err := net.Listen("tcp", localAddrStr)
	localAddr = ln.Addr()

	// Connect to server
	outLog.Printf("Connect to server at %s...", serverAddr)
	server, err = rpc.Dial("tcp", serverAddr)
	if err != nil {
		errLog.Printf("Failed to connected to server at %s", serverAddr)
		return
	}
	outLog.Printf("Successfully connected to server at %s!", serverAddr)
}

// Probe coordinator for liveliness
// Returns error if last heartbeat was more than hearbeatInterval
func getHeartbeat(coordinator dkvlib.CNodeConn, heartbeatInterval time.Duration) {
	lastHeartbeat := time.Now().UnixNano()
	for {
		heartbeat, err := coordinator.SendHeartbeat()
		if err != nil {
			if time.Now().UnixNano()-lastHeartbeat > int64(heartbeatInterval) {
				connected = false
				outLog.Println("Client disconnected from Coordinator. Please query Server for new coordinator.")
				return
			}
		} else {
			lastHeartbeat = heartbeat
		}

		// Query every tenth of a second
		time.Sleep(time.Millisecond * 100)
	}
}

func main() {
	gob.Register(&net.TCPAddr{})

	if len(os.Args) != 2 {
		fmt.Println("Usage: ", os.Args[0], "server")
		os.Exit(1)
	}
	serverAddr := os.Args[1]

	// connect to server
	ConnectServer(serverAddr)
	var msg int
	var response net.Addr

	// Get coordinator node address
	err := server.Call("KVServer.GetCoordinatorAddress", msg, &response)
	if err != nil {
		outLog.Println("Couldn't retrieve coordinator address:", err)
	}
	coordinatorNodeAddr := response
	outLog.Printf("Coordinator address received: %v\n", coordinatorNodeAddr)

	// Connect to coordinator API
	coordinator, error := dkvlib.OpenCoordinatorConn(coordinatorNodeAddr.String())
	if error != nil {
		outLog.Println("Couldn't connect to dkvlib:", error)
		return
	}
	connected = true

	// Call heartbeat probe function on API
	go getHeartbeat(coordinator, 2*time.Second)

	// Call functions on API
	outLog.Println("----------------- TEST: Write value -----------------")
	err = coordinator.Write("b", "7")
	if err != nil {
		outLog.Println(err)
	} else {
		outLog.Println("Write Succeeded")
	}

	outLog.Println("----------------- TEST: Read existing key -----------------")
	val, error := coordinator.Read("b")
	if error != nil {
		outLog.Println(error)
	} else {
		outLog.Printf("Value returned: %s", val)
	}

	outLog.Println("----------------- TEST: Delete existing key -----------------")
	err = coordinator.Delete("b")
	if err != nil {
		outLog.Println(err)
	} else {
		outLog.Println("Delete succeeded")
	}

	outLog.Println("----------------- TEST: Read non-existent key -----------------")
	val, error = coordinator.Read("a")
	if error != nil {
		outLog.Println(error)
	} else {
		outLog.Printf("Value returned: %s", val)
	}
	outLog.Println("----------------- TEST: Delete non-existent key -----------------")
	err = coordinator.Delete("a")
	if err != nil {
		outLog.Println("Delete failed")
	}

}
