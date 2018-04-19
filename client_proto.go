/*
Example client for the Regina and The Plastics KV Project
For use in Project 2 of UBC CPSC 416 2017W2

Usage:

$ go run client_proto.go [server ip:port] [cmd]
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
	"strings"
	"strconv"

	"./dkvlib"
)

var (
	errLog *log.Logger = log.New(os.Stderr, "[client] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog *log.Logger = log.New(os.Stderr, "[client] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
)

var (
	server              *rpc.Client
	coordinator         dkvlib.CNodeConn
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

func readCmd(cmdList []string) error {
	// Cmd args
	key := cmdList[0]

	// Execute
	val, err := coordinator.Read(key)
	if err != nil {
		outLog.Println("Read failed")
		return err
	} else {
		outLog.Printf("Value returned: %s", val)
	}

	return nil
}

func writeCmd(cmdList []string) error {
	// Cmd args
	key := cmdList[0]
	value := cmdList[1]

	// Execute
	err := coordinator.Write(key, value)
	if err != nil {
		outLog.Println("Write failed")
		return err
	}

	return nil
}

func deleteCmd(cmdList []string) error {
	// Cmd args
	key := cmdList[0]

	// Execute
	err := coordinator.Delete(key)
	if err != nil {
		outLog.Println("Delete failed")
		return err
	}

	return nil
}

func timeoutCmd(cmdList []string) error {
	// Cmd args
	timeoutString := cmdList[0]
	timeoutNum, _ := strconv.Atoi(timeoutString)

	// Execute
	outLog.Printf("Timeout for %d millisecond(s)...", timeoutNum)
	time.Sleep(time.Duration(timeoutNum) * time.Millisecond)

	return nil
}

func main() {
	gob.Register(&net.TCPAddr{})

	if len(os.Args) < 3 {
		outLog.Println("Usage: go run client.go [server ip:port] [cmd]")
		outLog.Printf("The following commands may be used:\nRead: R,<key>\nWrite: W,<key>,<value>\nDelete: D,<key>\n")
		os.Exit(1)
	}

	//////////// MOVE TO DKVLIB ////////// 
	// Note* Everything within this section should be analogous to a "MountDFS"
	// Connect to server
	serverAddr := os.Args[1]

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
	coordinator, err = dkvlib.OpenCoordinatorConn(coordinatorNodeAddr.String())
	if err != nil {
		outLog.Println("Couldn't connect to dkvlib:", err)
		return
	}
	connected = true

	// Call heartbeat probe function on API
	go getHeartbeat(coordinator, 2*time.Second)
	//////////// END //////////

	commands := os.Args[2:]

	outLog.Printf("Executing %d command actions...", len(commands))

	for _, cmd := range commands {
		cmdList := strings.Split(cmd, ",")
		switch cmdList[0] {
		case "R":
			if len(cmdList) != 2 {
				errLog.Println("Wrong number of command args! Aborting remaining commands")
				break
			} else {
				err = readCmd(cmdList[1:])
			}
		case "W":
			if len(cmdList) != 3 {
				errLog.Println("Wrong number of command args! Aborting remaining commands")
				break
			} else {
				err = writeCmd(cmdList[1:])
			}
		case "D":
			if len(cmdList) != 2 {
				errLog.Println("Wrong number of command args! Aborting remaining commands")
				break
			} else {
				err = deleteCmd(cmdList[1:])
			}
		case "T":
			if len(cmdList) != 2 {
				errLog.Println("Wrong number of command args! Aborting remaining commands")
				break
			} else {
				err = timeoutCmd(cmdList[1:])
			}
		default:
			errLog.Printf("%s is not an applicable command action. Aborting remaining commands")
			break
		}

		if err != nil {
			errLog.Println("Could not execute command: ", err)
			break
		}
	}

	outLog.Println("Commands complete!")


}