/*
Server for the Regina and The Plastics KV Project
For use in Project 2 of UBC CPSC 416 2017W2

Usage:

$ go run server.go
	-c Path to config.json file
*/

package main

import (
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/DistributedClocks/GoVector/govec"
)

////////////////////////////////////////////////////////////////////////////////
// ERRORS
////////////////////////////////////////////////////////////////////////////////

// ID has already been assigned in the server
type IDAlreadyRegisteredError string

func (e IDAlreadyRegisteredError) Error() string {
	return fmt.Sprintf("Server: ID already registered [%s]", string(e))
}

// Address already registered in the server
type AddressAlreadyRegisteredError string

func (e AddressAlreadyRegisteredError) Error() string {
	return fmt.Sprintf("Server: Address already registered [%s]", string(e))
}

// Misc. registration error
type RegistrationError string

func (e RegistrationError) Error() string {
	return fmt.Sprintf("Server: Failure to register node [%s]", string(e))
}

type InvalidFailureError string

func (e InvalidFailureError) Error() string {
	return fmt.Sprintf("Server: Failure Alert invalid. Ignoring. [%s]", string(e))
}

////////////////////////////////////////////////////////////////////////////////
// TYPES, VARIABLES, CONSTANTS
////////////////////////////////////////////////////////////////////////////////

// Variables related to general server function
var (
	config       Config
	errLog       *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog       *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	ServerLogger *govec.GoLog
)

// Variables related to nodes
var (
	allNodes           AllNodes = AllNodes{nodes: make(map[string]*Node)}
	currentCoordinator Node
	nextID             int = 0
	lastUpdate         int64
)

// Variables for failures
var (
	voteInPlace bool        /* block communication with client when true */
	allFailures AllFailures = AllFailures{nodes: make(map[string]bool)}
	allVotes    AllVotes    = AllVotes{votes: make(map[string]int)}
	voteTimeout int64
)

////////////////////////////////////////////////////////////////////////////////
// TYPES, STRUCTURES
////////////////////////////////////////////////////////////////////////////////
// Configuration
type Config struct {
	ServerAddress string       `json:"server-ip-port"`
	NodeSettings  NodeSettings `json:"node-settings"`
}

type RegistrationPackage struct {
	Settings      NodeSettings
	ID            string
	IsCoordinator bool
	LoggerInfo    []byte
}

// Node Settings
type NodeSettings struct {
	HeartBeat            uint32  `json:"heartbeat"`
	VotingWait           uint32  `json:"voting-wait"`
	ElectionWait         uint32  `json:"election-wait"`
	ServerUpdateInterval uint32  `json:"server-update-interval"`
	ReconnectionAttempts int     `json:"reconnection-attempts"`
	MajorityThreshold    float32 `json:"majority-threshold"`
}

// Node - a node of the network
type Node SmallNode
type SmallNode struct {
	ID            string
	IsCoordinator bool
	Address       net.Addr
	LoggerInfo    []byte
}

// All Nodes - a map containing all nodes, including the coordinator
type AllNodes struct {
	sync.RWMutex
	nodes map[string]*Node
}

type NodeInfo struct {
	ID         string
	Address    net.Addr
	LoggerInfo []byte
}

type CoordinatorFailureInfo struct {
	Failed         net.Addr
	Reporter       net.Addr
	NewCoordinator net.Addr
	LoggerInfo     []byte
}

type AllFailures struct {
	sync.RWMutex
	nodes map[string]bool
}

type AllVotes struct {
	sync.RWMutex
	votes map[string]int
}

// For RPC calls
type KVServer int

////////////////////////////////////////////////////////////////////////////////
// SERVER <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////

// Register a node into the KV node network
func (s *KVServer) RegisterNode(nodeInfo NodeInfo, settings *RegistrationPackage) error {
	ServerLogger.UnpackReceive("[Server] Add node to map", nodeInfo.LoggerInfo, &RegistrationPackage{})
	allNodes.Lock()
	defer allNodes.Unlock()

	// Temporary method for assigning an ID
	id := strconv.Itoa(nextID)

	// Increment ID
	nextID++

	// Define errors
	for _, node := range allNodes.nodes {
		if node.ID == id {
			return IDAlreadyRegisteredError(id)
		}
	}

	// Set node information and add to map
	allNodes.nodes[nodeInfo.Address.String()] = &Node{
		ID:            id,
		IsCoordinator: false,
		Address:       nodeInfo.Address,
	}
	// Check if this is the first node; if so set iscoordinator
	// and set current coordinator
	if len(allNodes.nodes) == 1 {
		allNodes.nodes[nodeInfo.Address.String()].IsCoordinator = true
		// Set current coordinator
		currentCoordinator = *allNodes.nodes[nodeInfo.Address.String()]
	}

	// Reply
	*settings = RegistrationPackage{Settings: config.NodeSettings,
		ID:            id,
		IsCoordinator: allNodes.nodes[nodeInfo.Address.String()].IsCoordinator}

	outLog.Printf("Got register from %s\n", nodeInfo.Address.String())
	outLog.Printf("Gave node ID %s\n", id)

	return nil
}

// GetAllNodes currently in the network
// *Useful if a heartbeat connection between nodes dies, but the network is still online
func (s *KVServer) GetAllNodes(msg int, response *map[string]*Node) error {
	outLog.Println("Updated node table.")
	allNodes.RLock()
	*response = allNodes.nodes
	allNodes.RUnlock()
	return nil
}

// Report failed network node
func (s KVServer) NodeFailureAlert(info *NodeInfo, _unused *int) error {
	failedNode := info.Address

	allNodes.Lock()
	defer allNodes.Unlock()

	outLog.Println("Node removed from system: ", allNodes.nodes[failedNode.String()].ID, "[", failedNode, "]")
	delete(allNodes.nodes, failedNode.String())

	return nil
}

// Incoming coordinator failure report from network node
func (s KVServer) ReportCoordinatorFailure(info *CoordinatorFailureInfo, _unused *int) error {
	failed := info.Failed
	reporter := info.Reporter
	voted := info.NewCoordinator

	reply := struct {
		unused     int
		LoggerInfo []byte
	}{}
	ServerLogger.UnpackReceive("[Server] Node reported coordinator failure", info.LoggerInfo, &reply)

	if currentCoordinator.Address.String() != failed.String() {
		outLog.Println("Reported failure not coordinator, ignore.")
		return InvalidFailureError(failed.String())
	}

	if len(allFailures.nodes) == 0 {
		allFailures.nodes = make(map[string]bool)
		voteInPlace = true
		outLog.Println("First reported failure of coordinator ", failed, " received from ", reporter)

		// First failure report, start listening for other reporting nodes
		allFailures.nodes[reporter.String()] = true

		// acknowledge vote
		if voted != nil {
			castVote(voted.String())
		}
		go DetectCoordinatorFailure(time.Now().UnixNano())

	} else {
		if _, ok := allFailures.nodes[reporter.String()]; !ok {
			outLog.Println("Reported failure of coordinator ", failed, " received from ", reporter)

			// if coordinator failure report has not yet been received by this reporter,
			// save report
			allFailures.nodes[reporter.String()] = true

			// save vote
			if voted != nil {
				castVote(voted.String())
			}
		}
	}

	return nil
}

// Listen for quorum number of failure reports
func DetectCoordinatorFailure(timestamp int64) {

	var didFail bool = false

	for time.Now().UnixNano() < timestamp+voteTimeout {
		allFailures.RLock()
		if len(allFailures.nodes) >= getQuorumNum()-1 { // coordinator does not take place in vote
			//quorum reached, coordinator failed
			didFail = true
			allFailures.RUnlock()
			break
		}
		allFailures.RUnlock()
	}

	if !didFail {
		// timeout, reports are invalid
		outLog.Println("Detecting coordinator failure timed out.  Failure reports invalid.")
		outLog.Println("Votes: ", len(allFailures.nodes))
		outLog.Println("Quorum: ", getQuorumNum()-1)

		// clear map of failures ad votes
		allFailures.nodes = make(map[string]bool)
		allVotes.votes = make(map[string]int)
		return
	}

	for {
		newCoordinatorAddr := ElectCoordinator()

		var newCoordinator Node
		var found bool = false
		for id, node := range allNodes.nodes {
			if id == "LoggerInfo" {
				continue
			}
			if node.Address.String() == newCoordinatorAddr {
				found = true
				node.IsCoordinator = true
				newCoordinator = *node
			}
		}

		if !found {
			errLog.Println("Could not find coordinator:", newCoordinatorAddr, ".Re-elect.")
			allVotes.Lock()
			delete(allVotes.votes, newCoordinatorAddr)
			allVotes.Unlock()
			continue
		}

		outLog.Println("Quorum reports of coordinator reached.", newCoordinator.ID, "[", newCoordinator.Address, "]")

		// Remove previous coordinator from all from list of nodes
		allNodes.Lock()
		delete(allNodes.nodes, currentCoordinator.Address.String())
		outLog.Println(currentCoordinator.ID, "[", currentCoordinator.Address, "]", "removed.")
		allNodes.Unlock()

		err := BroadcastCoordinator(newCoordinator)
		if err != nil {
			// Broadcasting to new coordinator failed, elect new coordinator
			outLog.Println("Broadcast failed.  Choosing new coordinator...")
			allNodes.Lock()

			delete(allNodes.nodes, newCoordinator.Address.String())

			allNodes.Unlock()
			allVotes.Lock()
			delete(allVotes.votes, newCoordinator.Address.String())
			allVotes.Unlock()

		} else {
			// Broadcast succeeded.
			outLog.Println("Broadcast of new coordinator succeeded.")
			currentCoordinator = newCoordinator

			// clear map of failures ad votes
			allFailures.nodes = make(map[string]bool)
			allVotes.votes = make(map[string]int)
			break
		}
	}
	voteInPlace = false
	return
}

////////////////////////////////////////////////////////////////////////////////
// COORDINATOR -> SERVER FUNCTIONS
////////////////////////////////////////////////////////////////////////////////

// Receive map of online nodes from coordinator
func (s *KVServer) GetOnlineNodes(args map[string]*Node, unused *int) (err error) {
	reply := struct {
		unused     int
		LoggerInfo []byte
	}{}
	ServerLogger.UnpackReceive("[Coordinator] Receive list of online nodes", args["LoggerInfo"].LoggerInfo, &reply)
	allNodes.Lock()
	allNodes.nodes = args
	allNodes.Unlock()

	// Update the last time the coordinator sent network info
	lastUpdate = time.Now().UnixNano()
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// COORDINATOR ELECTION
////////////////////////////////////////////////////////////////////////////////

func ElectCoordinator() string {
	allNodes.RLock()
	for id, node := range allNodes.nodes {
		if id == "LoggerInfo" {
			continue
		}
		if _, ok := allVotes.votes[node.Address.String()]; !ok {
			allVotes.Lock()
			allVotes.votes[node.Address.String()] = 0
			allVotes.Unlock()
		}
	}
	allNodes.RUnlock()

	allVotes.RLock()
	defer allVotes.RUnlock()

	var maxVotes int
	var electedCoordinator string
	mostPopular := []string{}

	for node, numVotes := range allVotes.votes {
		if len(mostPopular) == 0 { // append first node of list
			mostPopular = append(mostPopular, node)
			maxVotes = numVotes
		} else if numVotes > maxVotes { // if current node has more votes than the ones seen before, replace entire list with this node
			mostPopular = nil
			mostPopular = append(mostPopular, node)
		} else if numVotes == maxVotes { // if current node has the max number of notes, add to list
			mostPopular = append(mostPopular, node)
		}
	}

	if len(mostPopular) > 1 {
		// if there is a tie, elect randomly
		rand.Seed(time.Now().UnixNano())
		index := rand.Intn(len(mostPopular) - 1)
		electedCoordinator = mostPopular[index]
		outLog.Println("Tie exists.  Randomly elected new coordinator: ", electedCoordinator)
		return electedCoordinator
	}

	electedCoordinator = mostPopular[0]
	outLog.Println("New coordinator elected: ", electedCoordinator)
	return electedCoordinator
}

// Broadcasts new coordinator to all nodes in network
// Returns error if new coordinator fails to accept this role
func BroadcastCoordinator(newCoordinator Node) (err error) {
	outLog.Println("Broadcasting new coordinator.. ID: ", newCoordinator.ID)

	conn, err := rpc.Dial("tcp", newCoordinator.Address.String())
	if err != nil {
		errLog.Println("Error connecting to new coordinator", newCoordinator.ID, "[", newCoordinator.Address, "]")
		return err
	}

	args := NodeInfo{
		Address: newCoordinator.Address,
		ID:      newCoordinator.ID,
	}

	var reply int
	sendingMsg := ServerLogger.PrepareSend("[Server] Broadcast new Coordinator", args)
	args.LoggerInfo = sendingMsg
	err = conn.Call("KVNode.NewCoordinator", &args, &reply)
	if err != nil {
		errLog.Println("Error connecting to new coordinator", newCoordinator.ID, "[", newCoordinator.Address, "]")
		return err
	}

	// TODO: FOR TESTING. REMOVE
	// Uncomment to test case where after voting, Only Node A knows its the coordinator and dies before
	// other Nodes have saved Node A as the coordinator. Other nodes have no coordinator to send failure of Node A.
	//time.Sleep(5*time.Second)

	// Broadcast to all other nodes
	allNodes.RLock()
	defer allNodes.RUnlock()
	for id, node := range allNodes.nodes {
		if id == "LoggerInfo" {
			continue
		}
		if node.Address.String() == currentCoordinator.Address.String() || node.Address.String() == newCoordinator.Address.String() {
			continue
		}
		// Connect to node
		conn, err := rpc.Dial("tcp", node.Address.String())
		if err != nil {
			errLog.Println("Error sending new coordinator to ", node.ID, "[", node.Address, "]")
			continue
		}

		args := NodeInfo{
			Address: newCoordinator.Address,
			ID:      newCoordinator.ID,
		}
		var reply int

		sendingMsg2 := ServerLogger.PrepareSend("[Server] Broadcast new Coordinator", args)
		args.LoggerInfo = sendingMsg2

		err = conn.Call("KVNode.NewCoordinator", &args, &reply)
		if err != nil {
			errLog.Println("Error broadcasting new coordinator to ", node.ID, "[", node.Address, "]")
		}
	}

	return nil

}

////////////////////////////////////////////////////////////////////////////////
// SERVER <-> CLIENT FUNCTIONS
////////////////////////////////////////////////////////////////////////////////

// GetCoordinatorAddress sends coord address
func (s *KVServer) GetCoordinatorAddress(msg int, response *net.Addr) error {
	for {
		if !voteInPlace {
			*response = currentCoordinator.Address
			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// MAIN, LOCAL
////////////////////////////////////////////////////////////////////////////////

func readConfigOrDie(path string) {
	file, err := os.Open(path)
	handleErrorFatal("config file", err)

	buffer, err := ioutil.ReadAll(file)
	handleErrorFatal("read config", err)

	err = json.Unmarshal(buffer, &config)
	handleErrorFatal("parse config", err)
}

func getQuorumNum() int {
	if len(allNodes.nodes) <= 2 {
		return 1
	}
	return len(allNodes.nodes)/2 + 1
}

func castVote(addr string) {
	allVotes.Lock()
	defer allVotes.Unlock()

	if _, ok := allVotes.votes[addr]; ok {
		allVotes.votes[addr]++
	} else {
		allVotes.votes[addr] = 1
	}

	outLog.Println("Vote for ", addr, " casted.")
}

// Remove all nodes in the server if no network updates have occurred in a while
func MonitorCoordinator() {
	for {
		if lastUpdate != 0 {
			currentTime := time.Now().UnixNano()

			if currentTime-lastUpdate > int64(time.Duration(config.NodeSettings.VotingWait)*time.Millisecond) && !voteInPlace {
				allNodes.RLock()
				size := len(allNodes.nodes)
				allNodes.RUnlock()

				if size == 1 {
					outLog.Println("No updates received from coordinator. Purging network info from server...")
					allNodes.Lock()
					allNodes.nodes = make(map[string]*Node)
					allNodes.Unlock()
					lastUpdate = 0
				} else {
					// Within election period, if no election takes place, purge everything
					allgood := false
					for time.Now().UnixNano() < currentTime+voteTimeout {
						if time.Now().UnixNano()-lastUpdate < int64(5*time.Second) || voteInPlace {
							allgood = true
							break
						}
					}
					if !allgood {
						outLog.Println("No updates received from coordinator. Purging network info from server...")
						allNodes.Lock()
						allNodes.nodes = make(map[string]*Node)
						allNodes.Unlock()
						lastUpdate = 0
					}
				}
			}
		}
		time.Sleep(time.Duration(config.NodeSettings.HeartBeat) * time.Millisecond)
	}
}

func main() {
	ServerLogger = govec.InitGoVector("Server", "LogFile-Server")

	gob.Register(&net.TCPAddr{})

	path := flag.String("c", "", "Path to the JSON config")
	flag.Parse()

	if *path == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	readConfigOrDie(*path)
	voteTimeout = int64(time.Duration(config.NodeSettings.VotingWait) * time.Millisecond)

	rand.Seed(time.Now().UnixNano())

	kvserver := new(KVServer)

	server := rpc.NewServer()
	server.Register(kvserver)

	l, e := net.Listen("tcp", config.ServerAddress)

	handleErrorFatal("listen error", e)
	outLog.Printf("Server started. Receiving on %s\n", config.ServerAddress)

	go MonitorCoordinator()

	for {
		conn, _ := l.Accept()
		go server.ServeConn(conn)
	}
}

func handleErrorFatal(msg string, e error) {
	if e != nil {
		errLog.Fatalf("%s, err = %s\n", msg, e.Error())
	}
}
