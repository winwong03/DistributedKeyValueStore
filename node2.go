/*
Network node for the Regina and The Plastics KV Project
For use in Project 2 of UBC CPSC 416 2017W2

Usage:

$ go run [server ip:port]
*/

package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"./dkvlib"
	"github.com/DistributedClocks/GoVector/govec"
)

////////////////////////////////////////////////////////////////////////////////
// ERRORS
////////////////////////////////////////////////////////////////////////////////

// Contains key
type InvalidKeyError string

func (e InvalidKeyError) Error() string {
	return fmt.Sprintf("Node: Invalid key [%s]", string(e))
}

// Contains serverAddr
type DisconnectedServerError string

func (e DisconnectedServerError) Error() string {
	return fmt.Sprintf("Node: Cannot connect to server [%s]", string(e))
}

type InvalidFailureError string

func (e InvalidFailureError) Error() string {
	return fmt.Sprintf("Server: Failure Alert invalid. Ignoring. [%s]", string(e))
}

type InvalidPermissionsError string

func (e InvalidPermissionsError) Error() string {
	return fmt.Sprintf("Node: Network node attempting to take on Coordinator task [%s]", string(e))
}

////////////////////////////////////////////////////////////////////////////////
// TYPES, VARIABLES, CONSTANTS
////////////////////////////////////////////////////////////////////////////////

// Variables related to general node function
var (
	errLog                    *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog                    *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	goVectorNetworkNodeLogger *govec.GoLog
)

// Variable related to the node
var (
	LocalAddr         net.Addr
	ServerAddr        string
	Server            *rpc.Client
	Coordinator       *Node
	allNodes          AllNodes = AllNodes{nodes: make(map[string]*Node)}
	isCoordinator     bool
	Settings          NodeSettings
	ID                string
	coordinatorFailed bool    = false
	kvstore           KVStore = KVStore{store: make(map[string]string)}
)

// For coordinator
var (
	allFailures AllFailures = AllFailures{nodes: make(map[string]*FailedNode)}
	voteTimeout int64
)

////////////////////////////////////////////////////////////////////////////////
// TYPES, STRUCTURES
////////////////////////////////////////////////////////////////////////////////
type KVStore struct {
	sync.RWMutex
	store map[string]string
}

// Registration package
type RegistrationPackage struct {
	Settings      NodeSettings
	ID            string
	IsCoordinator bool
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

// Node Settings
type Node struct {
	ID              string
	IsCoordinator   bool
	Address         net.Addr
	RecentHeartbeat int64
	NodeConn        *rpc.Client
}

// Node Settings Known By Server
type SmallNode struct {
	ID            string
	IsCoordinator bool
	Address       net.Addr
	LoggerInfo    []byte
}

// All Nodes
type AllNodes struct {
	sync.RWMutex
	nodes map[string]*Node
}

type AllFailures struct {
	sync.RWMutex
	nodes map[string]*FailedNode
}

type FailedNode struct {
	timestamp int64
	address   net.Addr
	reporters map[string]bool
}

type NodeInfo struct {
	ID         string
	Address    net.Addr
	LoggerInfo []byte
}

type FailureInfo struct {
	Failed     net.Addr
	Reporter   net.Addr
	LoggerInfo []byte
}

type CoordinatorFailureInfo struct {
	Failed         net.Addr
	Reporter       net.Addr
	NewCoordinator net.Addr
	LoggerInfo     []byte
}

// For RPC Calls
type KVNode int

type ReadRequest struct {
	Key        string
	LoggerInfo []byte
}

type ReadReply struct {
	Value      string
	Success    bool
	LoggerInfo []byte
}

type WriteRequest struct {
	Key        string
	Value      string
	LoggerInfo []byte
}

type DeleteRequest struct {
	Key        string
	LoggerInfo []byte
}

type OpReply struct {
	Success    bool
	LoggerInfo []byte
}

////////////////////////////////////////////////////////////////////////////////
// SERVER <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////
// Connects to the server to join or initiate the KV network
func ConnectServer(serverAddr string) {
	// Look up local addr to use for this node
	var localAddr string
	localHostName, _ := os.Hostname()
	listOfAddr, _ := net.LookupIP(localHostName)
	for _, addr := range listOfAddr {
		if ok := addr.To4(); ok != nil {
			localAddr = ok.String()
		}
	}
	localAddr = fmt.Sprintf("%s%s", localAddr, ":0")
	ln, err := net.Listen("tcp", localAddr)
	if err != nil {
		outLog.Printf("Failed to get a local addr:%s\n", err)
		return
	}
	LocalAddr = ln.Addr()

	// Connect to server
	outLog.Printf("Connect to server at %s...", serverAddr)
	Server, err = rpc.Dial("tcp", serverAddr)
	if err != nil {
		errLog.Printf("Failed to connected to server at %s", serverAddr)
		return
	}
	outLog.Printf("Successfully connected to server at %s!", serverAddr)
	outLog.Printf("My local address is %s", LocalAddr.String())

	// Register node to server
	err = RegisterNode()
	if err != nil {
		errLog.Println("Failed to register node")
		return
	}
	outLog.Println("Successfully registered node")

	// Connect to existing nodes
	GetNodes()
	outLog.Println("Connected to all existing nodes")

	// Save address to reconnect
	ServerAddr = serverAddr

	// Close server connection
	outLog.Printf("Closing connection to server...")
	err = Server.Close()
	if err != nil {
		outLog.Println("Server connection already closing:", err)
	} else {
		outLog.Printf("Server connection successfully closed! Node is ready!")
	}

	// Coordinator reports for duty to server
	if isCoordinator {
		ReportForCoordinatorDuty(serverAddr)
	}

	// Listen for other incoming nodes
	kvNode := new(KVNode)
	node := rpc.NewServer()
	node.Register(kvNode)

	for {
		conn, _ := ln.Accept()
		go node.ServeConn(conn)
	}
}

// Registers a node to the server and receives a node ID
func RegisterNode() (err error) {
	goVectorNetworkNodeLogger = govec.InitGoVector("Node"+LocalAddr.String(), "LogFile"+LocalAddr.String())
	var regInfo RegistrationPackage

	nodeInfo := NodeInfo{Address: LocalAddr}

	sendingMsg := goVectorNetworkNodeLogger.PrepareSend("[Node"+ID+"] Node joined the network, send Register to Server", nodeInfo)
	nodeInfo.LoggerInfo = sendingMsg
	err = Server.Call("KVServer.RegisterNode", nodeInfo, &regInfo)
	if err != nil {
		outLog.Println("Something bad happened:", err)
		return err
	}

	// Store node settings from server
	Settings = regInfo.Settings
	voteTimeout = int64(time.Millisecond * time.Duration(Settings.VotingWait))
	ID = regInfo.ID
	isCoordinator = regInfo.IsCoordinator

	if isCoordinator {
		outLog.Printf("Received node ID %s and this node is the coordinator!", ID)
		goVectorNetworkNodeLogger.LogLocalEvent("[Node" + ID + "] I am the coordinator!")
	} else {
		outLog.Printf("Received node ID %s and this node is a network node", ID)
		goVectorNetworkNodeLogger.LogLocalEvent("[Node" + ID + "] I am a network node!")
	}

	return nil
}

// Retrieves all nodes existing in network
func GetNodes() (err error) {
	var nodeSet map[string]*Node

	err = Server.Call("KVServer.GetAllNodes", 0, &nodeSet)
	if err != nil {
		outLog.Println("Error getting existing nodes from server")
	} else {
		outLog.Println("Connecting to the other nodes...")
		for id, node := range nodeSet {
			if id != "LoggerInfo" {
				if node.Address.String() != LocalAddr.String() {
					ConnectNode(node)
				}
			}
		}
	}

	// Add self to allNodes map
	AddSelfToMap()

	kvstore.Lock()
	kvstore.store["a"] = "0"
	kvstore.store["b"] = "0"
	kvstore.store["c"] = "0"
	kvstore.Unlock()
	return nil
}

// Add self to allNodes map
func AddSelfToMap() {
	selfNode := &Node{
		ID:            ID,
		IsCoordinator: isCoordinator,
		Address:       LocalAddr,
	}
	allNodes.Lock()
	allNodes.nodes[LocalAddr.String()] = selfNode
	allNodes.Unlock()
}

// Report node failure to coordinator
func ReportNodeFailure(node *Node) {
	info := &FailureInfo{
		Failed:   node.Address,
		Reporter: LocalAddr,
	}
	var reply int

	if _, ok := allNodes.nodes[Coordinator.Address.String()]; !ok {
		ReportCoordinatorFailure(Coordinator)
	}

	sendingMsg := goVectorNetworkNodeLogger.PrepareSend("[Node"+ID+"] Reporting a node failure to Coordinator", info)
	info.LoggerInfo = sendingMsg
	err := Coordinator.NodeConn.Call("KVNode.ReportNodeFailure", &info, &reply)
	if err != nil {
		outLog.Println("Error reporting failure of node ", node.ID, "[", node.Address, "].  Report coordinator failure.")
		ReportCoordinatorFailure(Coordinator)
	}
}

func ReportCoordinatorFailure(node *Node) {
	// If connection with server has failed, reconnect
	Server, err := rpc.Dial("tcp", ServerAddr)
	if err != nil {
		outLog.Println("Failed to reconnect with server.")
		return
	}

	outLog.Println("Reconnected with server to vote for failure of coordinator.")

	vote := voteNewCoordinator()

	allNodes.RLock()
	if _, ok := allNodes.nodes[node.Address.String()]; !ok {
		outLog.Println("Node does not exist.  Aborting coordinator failure alert.")
		allNodes.RUnlock()
		return
	}
	allNodes.RUnlock()

	info := &CoordinatorFailureInfo{
		Failed:         node.Address,
		Reporter:       LocalAddr,
		NewCoordinator: vote,
	}

	var reply int

	sending := goVectorNetworkNodeLogger.PrepareSend("[Node"+ID+"] Reporting Coordinator Failure", info)
	info.LoggerInfo = sending
	err = Server.Call("KVServer.ReportCoordinatorFailure", &info, &reply)
	if err != nil {
		outLog.Println("Error reporting failure of coordinator ", node.ID, "[", node.Address, "]")
	} else {
		coordinatorFailed = true
	}
}

// Save the new coordinator
func (n KVNode) NewCoordinator(args *NodeInfo, _unused *int) (err error) {
	reply := struct {
		unused     int
		LoggerInfo []byte
	}{}
	goVectorNetworkNodeLogger.UnpackReceive("[Node"+ID+"] received new Coordinator", args.LoggerInfo, &reply)
	addr := args.Address
	outLog.Println("Received new coordinator... ")

	allNodes.Lock()
	if Coordinator != nil {
		if _, ok := allNodes.nodes[Coordinator.Address.String()]; ok {
			delete(allNodes.nodes, Coordinator.Address.String())
		}
	}
	allNodes.Unlock()

	if addr.String() == LocalAddr.String() {
		outLog.Println("I am the new coordinator!")
		allNodes.nodes[addr.String()].IsCoordinator = true
		isCoordinator = true
		goVectorNetworkNodeLogger.LogLocalEvent("[Node" + ID + "] I am the new coordinator")
		Server.Close()
		ReportForCoordinatorDuty(ServerAddr)
	} else {
		// TODO: TO TEST
		// Uncomment to test case where new coordinator dies before rest of network knows its the new coordinator
		//time.Sleep(5 * time.Second)

		if _, ok := allNodes.nodes[addr.String()]; ok {
			outLog.Println("Node exists", addr.String())
			allNodes.nodes[addr.String()].IsCoordinator = true
			Coordinator = allNodes.nodes[addr.String()]
			outLog.Println(Coordinator.ID, "[", Coordinator.Address, "]", " is the new coordinator.")
			Server.Close()
		} else {
			outLog.Println("Node does not exist", addr.String())
			Server.Close()
			return err
		}

	}

	// election complete, new coordinator elected
	coordinatorFailed = false
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////

// Check for heartbeat timeouts from other nodes
func MonitorHeartBeats(addr string) {
	for {
		time.Sleep(time.Duration(Settings.HeartBeat+1000) * time.Millisecond)
		if _, ok := allNodes.nodes[addr]; ok {

			if time.Now().UnixNano()-allNodes.nodes[addr].RecentHeartbeat > int64(Settings.HeartBeat)*int64(time.Millisecond) {
				if isCoordinator {
					SaveNodeFailure(allNodes.nodes[addr])
				} else if allNodes.nodes[addr].IsCoordinator && coordinatorFailed == false {
					ReportCoordinatorFailure(allNodes.nodes[addr])
				} else {
					ReportNodeFailure(allNodes.nodes[addr])
				}
			}
		} else {
			outLog.Println("Node not found in network. Stop monitoring heartbeats.", addr)
			return
		}

	}
}

// Broadcast of node failure from coordinator
func (n KVNode) NodeFailureAlert(node *NodeInfo, _unused *int) error {
	reply := struct {
		unused     int
		LoggerInfo []byte
	}{}
	goVectorNetworkNodeLogger.UnpackReceive("[Node"+ID+"] Received confirmed Node failure report", node.LoggerInfo, &reply)
	outLog.Println(" Node failure alert received from coordinator:  ", node.ID, "[", node.Address, "]")

	allNodes.Lock()
	defer allNodes.Unlock()

	// remove node from list of nodes
	delete(allNodes.nodes, node.Address.String())
	outLog.Println(" Node successfully removed: ", node.ID, "[", node.Address, "]")
	return nil
}

// Request KVstore values from the coordinator
func GetValuesFromCoordinator() {
	var unused int
	var reply map[string]string

	outLog.Println("Requesting map values from coordinator...")

	err := Coordinator.NodeConn.Call("KVNode.RequestValues", unused, &reply)
	if err != nil {
		outLog.Printf("Could not retrieve kvstore values from coordinator: %s\n", err)
	}

	kvstore.Lock()
	kvstore.store = reply
	kvstore.Unlock()

	kvstore.RLock()
	outLog.Printf("This is the map:%v\n", kvstore.store)
	kvstore.RUnlock()
}

////////////////////////////////////////////////////////////////////////////////
// COORDINATOR FUNCTION
////////////////////////////////////////////////////////////////////////////////

// Return kvstore to requesting node
func (n KVNode) RequestValues(unused int, reply *map[string]string) error {
	if !isCoordinator {
		handleErrorFatal("Network node attempting to run coordinator node function.", nil)
	}
	outLog.Println("Coordinator retrieving majority values for new node...")
	kvstore.RLock()
	*reply = kvstore.store
	kvstore.RUnlock()
	return nil
}

// Report to server as coordinator
func ReportForCoordinatorDuty(serverAddr string) {
	// Connect to server
	outLog.Printf("Coordinator connecting to server at %s...", serverAddr)
	conn, err := rpc.Dial("tcp", serverAddr)
	if err != nil {
		errLog.Printf("Coordinator failed to connected to server at %s", serverAddr)
		return
	}
	outLog.Printf("Coordinator successfully connected to server at %s!", serverAddr)
	Server = conn

	// Periodically update server about online nodes
	go UpdateOnlineNodes()
}

// Attempt to reconnect
func ReconnectServer(serverAddr string) (err error) {
	var conn *rpc.Client
	if !isCoordinator {
		handleErrorFatal("Not a network node function.", InvalidPermissionsError(LocalAddr.String()))
		return err
	}

	var numAttempts = Settings.ReconnectionAttempts

	for i := 0; i < numAttempts; i++ {
		outLog.Println("Attempting to reconnect to server for the ", i, "time")
		conn, err = rpc.Dial("tcp", serverAddr)
		if err == nil {
			outLog.Println("Reconnection succeeded.")
			Server = conn
			return nil
		}
		time.Sleep(2 * time.Second)
	}

	return err
}

// Resign from coordinator role
func CoordinatorResign() {
	if !isCoordinator {
		handleErrorFatal("Not a network node function.", InvalidPermissionsError(LocalAddr.String()))
		return
	}
	outLog.Println("Cannot connect to server.  I am resigning from the coordinator role!")

	// Tell all network nodes to start vote
	for id, node := range allNodes.nodes {
		if id == "LoggerInfo" {
			continue
		}

		if node.Address == LocalAddr {
			continue
		}

		args := &FailureInfo{
			Reporter: LocalAddr,
			Failed:   LocalAddr,
		}
		var reply int
		err := node.NodeConn.Call("KVNode.CoordinatorResign", &args, &reply)
		if err != nil {
			errLog.Println("Could not tell node coordinator is resigning. ", node.ID, "[", node.Address, "]")
		}
	}
}

// Report connected nodes to server
func UpdateOnlineNodes() {
	for {
		var unused int

		allNodes.RLock()
		nodes := allNodes.nodes
		allNodes.RUnlock()

		nodeMap := make(map[string]*SmallNode)
		for k, v := range nodes {
			nodeMap[k] = &SmallNode{
				ID:            v.ID,
				IsCoordinator: v.IsCoordinator,
				Address:       v.Address,
			}
		}

		sendingMsg := goVectorNetworkNodeLogger.PrepareSend("[Node"+ID+"] Coordinator sends update of nodes to Server", nodeMap)
		nodeMap["LoggerInfo"] = &SmallNode{
			LoggerInfo: sendingMsg,
		}
		err := Server.Call("KVServer.GetOnlineNodes", nodeMap, &unused)
		if err != nil {
			outLog.Printf("Could not send online nodes to server: %s\n", err)

			if ReconnectServer(ServerAddr) != nil {
				CoordinatorResign()
				return
			}

		} else {
			//outLog.Println("Server's map of online nodes updated...")
		}
		time.Sleep(time.Duration(Settings.ServerUpdateInterval) * time.Millisecond)
	}
}

// Coordinator has observed failure of a node
func SaveNodeFailure(node *Node) {
	addr := node.Address

	if !isCoordinator {
		handleErrorFatal("Not a network node function.", InvalidPermissionsError(LocalAddr.String()))
	}
	allFailures.Lock()
	if node, ok := allFailures.nodes[addr.String()]; ok {
		node.reporters[LocalAddr.String()] = true
		allFailures.Unlock()
	} else {
		reporters := make(map[string]bool)
		reporters[LocalAddr.String()] = true
		allFailures.nodes[addr.String()] = &FailedNode{
			timestamp: time.Now().UnixNano(),
			address:   addr,
			reporters: reporters,
		}
		allFailures.Unlock()
		go DetectFailure(addr, allFailures.nodes[addr.String()].timestamp)
	}

}

// Node failure report from network node
func (n KVNode) ReportNodeFailure(info *FailureInfo, _unused *int) error {
	failure := info.Failed
	reporter := info.Reporter

	reply := struct {
		unused     int
		LoggerInfo []byte
	}{}
	goVectorNetworkNodeLogger.UnpackReceive("[Node"+ID+"] Coordinator received node failure report", info.LoggerInfo, &reply)

	outLog.Println("Failed node ", failure, " detected by ", reporter)

	allFailures.Lock()
	if node, ok := allFailures.nodes[failure.String()]; ok {
		if _, ok := node.reporters[reporter.String()]; !ok {
			node.reporters[reporter.String()] = true
		}
		outLog.Println(len(node.reporters), "votes received for ", failure)
		allFailures.Unlock()
	} else {
		// Check that this node actually exists
		allFailures.Unlock()
		allNodes.RLock()
		if _, ok := allNodes.nodes[failure.String()]; !ok {
			outLog.Println("Node does not exist in network.  Ignoring failure report.")
			allNodes.RUnlock()
			return InvalidFailureError(failure.String())
		}

		// first detection of failure
		reporters := make(map[string]bool)
		reporters[reporter.String()] = true
		outLog.Println("First failure of ", failure)

		allFailures.nodes[failure.String()] = &FailedNode{
			timestamp: time.Now().UnixNano(),
			address:   failure,
			reporters: reporters,
		}

		allNodes.RUnlock()

		go DetectFailure(failure, allFailures.nodes[failure.String()].timestamp)
	}

	return nil
}

// Begin listening for failure reports for given node
func DetectFailure(failureAddr net.Addr, timestamp int64) {
	quorum := getQuorumNum()

	// if time window has passed, and quorum not reached, failure is considered invalid
	for time.Now().UnixNano() < timestamp+voteTimeout {
		allFailures.RLock()
		if len(allFailures.nodes[failureAddr.String()].reporters) >= quorum {
			outLog.Println("Quorum votes on failure reached for ", failureAddr.String())
			allFailures.RUnlock()

			// Remove from pending failures
			allFailures.Lock()
			if _, ok := allFailures.nodes[failureAddr.String()]; ok {
				delete(allFailures.nodes, failureAddr.String())
			}
			allFailures.Unlock()

			RemoveNode(failureAddr)
			return
		}
		allFailures.RUnlock()
		time.Sleep(time.Millisecond)
	}

	// Remove from pending failures
	allFailures.Lock()
	delete(allFailures.nodes, failureAddr.String())
	allFailures.Unlock()
	// TODO: TELL NODES TO RECONNECT?
	outLog.Println("Timeout reached.  Failure invalid for ", allNodes.nodes[failureAddr.String()].ID, "[", failureAddr, "]")
}

// Remove node from network
func RemoveNode(node net.Addr) {
	goVectorNetworkNodeLogger.LogLocalEvent("[Node" + ID + "] Quorum for Node failure reached by coordinator")
	outLog.Println("Removing ", node)
	allNodes.Lock()
	if _, ok := allNodes.nodes[node.String()]; ok {
		delete(allNodes.nodes, node.String())
	}
	allNodes.Unlock()

	allNodes.RLock()
	defer allNodes.RUnlock()

	// send broadcast to all network nodes declaring node failure
	var reply int
	args := &NodeInfo{
		Address: node,
	}
	for id, n := range allNodes.nodes {
		if id == "LoggerInfo" {
			continue
		}
		if n.Address.String() == LocalAddr.String() {
			// Do not send notice to self
			continue
		}
		sendingMsg := goVectorNetworkNodeLogger.PrepareSend("[Node"+ID+"] Coordinator sends failed node confirmation to all nodes", args)
		args.LoggerInfo = sendingMsg
		err := n.NodeConn.Call("KVNode.NodeFailureAlert", &args, &reply)
		if err != nil {
			outLog.Println("Failure broadcast failed to ", n.ID, "[", n.Address, "]")
		}
	}

	// send failure acknowledgement to server
	err := Server.Call("KVServer.NodeFailureAlert", &args, &reply)
	if err != nil {
		outLog.Println("Failure alert to server failed", err)
	}
}

// Returns quorum: num nodes / 2 + 1 or 1 if num nodes == 2
func getQuorumNum() int {
	if !isCoordinator {
		handleErrorFatal("Not a network node function.", nil)
	}
	allNodes.RLock()
	defer allNodes.RUnlock()
	size := len(allNodes.nodes)/2 + 1
	if size <= 2 {
		return 1
	}
	return size
}

// Vote for who they think should be the new coordinator
func voteNewCoordinator() net.Addr {

	lowestID, err := strconv.Atoi(ID) // get current node's id
	if err != nil {
		outLog.Println("Error retrieving local id.")
	}
	vote := LocalAddr

	allNodes.RLock()
	defer allNodes.RUnlock()
	// Look for the node with the lowest ID
	for id, node := range allNodes.nodes {
		if id == "LoggerInfo" {
			continue
		}
		if node.IsCoordinator {
			continue // Do not vote for current coordinator
		}
		id, err := strconv.Atoi(node.ID)
		if err != nil {
			outLog.Println("Error retreiving node ID. ", node.Address, " ID: ", node.ID)
		}

		if id < lowestID {
			lowestID = id
			vote = node.Address
		}
	}

	outLog.Println("Voting for ", lowestID, "[", vote, "] as new coordinator.")
	return vote
}

func (n KVNode) SendHeartbeat(unused_args *int, reply *int64) error {
	//outLog.Println("Heartbeat request received from client.")
	*reply = time.Now().UnixNano()
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// COORDINATOR NODE <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////

// Read request to coordinator
func (n KVNode) CoordinatorRead(args ReadRequest, reply *ReadReply) error {
	outLog.Println("Coordinator received read operation")

	// ask all nodes for their values (vote)
	// map of values to their count
	valuesMap := make(map[string]int)
	quorum := getQuorumNum()

	// add own value to map if it exists
	kvstore.RLock()
	_, ok := kvstore.store[args.Key]
	if ok {
		addToValuesMap(valuesMap, kvstore.store[args.Key])
	}
	kvstore.RUnlock()

	allNodes.Lock()
	outLog.Println("Attempting to read back-up nodes...")
	for id, node := range allNodes.nodes {
		if id == "LoggerInfo" {
			continue
		}
		if !node.IsCoordinator {
			outLog.Printf("Reading from node %s...\n", node.ID)

			nodeArgs := args
			nodeReply := ReadReply{}

			//GoVector Logging Prepare a Message
			sendingMsg := goVectorNetworkNodeLogger.PrepareSend("[Node"+ID+"] Coordinator sending Read to Node", nodeArgs)
			nodeArgs.LoggerInfo = sendingMsg
			err := node.NodeConn.Call("KVNode.NodeRead", nodeArgs, &nodeReply)
			if err != nil {
				outLog.Println("Could not reach node ", node.ID)
			}

			goVectorNetworkNodeLogger.UnpackReceive("[Node"+ID+"] Coordinator receives Ack/Nack from Node", nodeReply.LoggerInfo, &ReadReply{})

			// Record successes
			if !nodeReply.Success {
				outLog.Printf("Failed to read from node %s... \n", node.ID)
			} else {
				outLog.Printf("Successfully read from node %s...\n", node.ID)
				addToValuesMap(valuesMap, nodeReply.Value)
			}
		}

	}

	allNodes.Unlock()

	// Find value with majority and set in local kvstore
	// If no majority value exists, return success - false
	var results []string
	largestCount := 0
	for val, count := range valuesMap {
		if count > largestCount {
			largestCount = count
			results = nil
			results = append(results, val)
		} else if count == largestCount {
			results = append(results, val)
		}
	}
	outLog.Printf("LENGTH: %d\n", len(results))

	if largestCount >= quorum {
		var result string
		if len(results) == 1 {
			result = results[0]
		} else {
			result = results[rand.Intn(len(results)-1)]
		}
		// update coordinator kvstore
		kvstore.Lock()
		kvstore.store[args.Key] = result
		kvstore.Unlock()
		outLog.Println("Sending Read decision to back-up nodes")
		sendWriteToNodes(args.Key, result)
		reply.Value = result
		reply.Success = true

	} else {
		// update coordinator kvstore
		kvstore.Lock()
		delete(kvstore.store, args.Key)
		kvstore.Unlock()
		outLog.Println("Sending Delete to back-up nodes")
		sendDeleteToNodes(args.Key)
		reply.Value = ""
		reply.Success = false
	}

	return nil
}

func sendWriteToNodes(key string, value string) {
	allNodes.Lock()
	defer allNodes.Unlock()

	for id, node := range allNodes.nodes {
		if id == "LoggerInfo" {
			continue
		}
		if !node.IsCoordinator {
			outLog.Printf("Writing to node %s...\n", node.ID)

			nodeArgs := WriteRequest{
				Key:   key,
				Value: value,
			}
			nodeReply := OpReply{}

			err := node.NodeConn.Call("KVNode.NodeWrite", nodeArgs, &nodeReply)
			if err != nil {
				outLog.Println("Could not write to node ", err)
			}

			// Record successes
			if nodeReply.Success {
				outLog.Printf("Successfully wrote to node %s!\n", node.ID)
			} else {
				outLog.Printf("Failed to write to node %s...\n", node.ID)
			}
		}
	}
}

func sendDeleteToNodes(key string) {
	allNodes.Lock()
	defer allNodes.Unlock()

	// Attempt delete from backup nodes
	for id, node := range allNodes.nodes {
		if id == "LoggerInfo" {
			continue
		}
		if !node.IsCoordinator {
			outLog.Printf("Deleting from node %s...\n", node.ID)

			nodeArgs := DeleteRequest{
				Key: key,
			}
			nodeReply := OpReply{}

			err := node.NodeConn.Call("KVNode.NodeDelete", nodeArgs, &nodeReply)
			if err != nil {
				outLog.Println("Could not delete from node ", err)
			}

			// Record successes
			if nodeReply.Success {
				outLog.Printf("Successfully deleted from node %s!\n", node.ID)
			} else {
				outLog.Printf("Failed to delete from node %s...\n", node.ID)
			}
		}

	}

}

func addToValuesMap(valuesMap map[string]int, val string) {
	_, ok := valuesMap[val]
	if ok {
		valuesMap[val] = valuesMap[val] + 1
	} else {
		valuesMap[val] = 1
	}
}

func (n KVNode) NodeRead(args ReadRequest, reply *ReadReply) error {
	goVectorNetworkNodeLogger.UnpackReceive("[Node"+ID+" ]: Receiving Read from Coordinator", args.LoggerInfo, &ReadRequest{})

	outLog.Println("Node received Read operation")
	kvstore.RLock()
	val, ok := kvstore.store[args.Key]

	kvstore.RUnlock()
	if !ok {
		reply.Success = false
		reply.Value = ""
		goVectorNetworkNodeLogger.PrepareSend("[Node"+ID+" ]: Sending Ack/Nack to Coordinator", reply)
	} else {
		reply.Success = true
		reply.Value = val
		goVectorNetworkNodeLogger.PrepareSend("[Node"+ID+" ]: Sending Ack/Nack to Coordinator", reply)
	}
	return nil
}

// Writing a KV pair to the coordinator node
func (n KVNode) CoordinatorWrite(args WriteRequest, reply *OpReply) error {

	allNodes.Lock()
	defer allNodes.Unlock()

	// Attempt to write to backup nodes
	successes := 0
	outLog.Println("Attempting to write to back-up nodes...")
	for id, node := range allNodes.nodes {
		if id == "LoggerInfo" {
			continue
		}
		if !node.IsCoordinator {
			outLog.Printf("Writing to node %s...\n", node.ID)

			nodeArgs := args
			nodeReply := OpReply{}

			//GoVector Logging Prepare a Message
			sendingMsg := goVectorNetworkNodeLogger.PrepareSend("[Node"+ID+"] Coordinator sending Write to Node", nodeArgs)
			nodeArgs.LoggerInfo = sendingMsg
			err := node.NodeConn.Call("KVNode.NodeWrite", nodeArgs, &nodeReply)
			if err != nil {
				outLog.Println("Could not write to node ", err)
			}
			goVectorNetworkNodeLogger.UnpackReceive("[Node"+ID+"] Coordinator receives Ack/Nack from node", nodeReply.LoggerInfo, &OpReply{})

			// Record successes
			if nodeReply.Success {
				successes++
				outLog.Printf("Successfully wrote to node %s!\n", node.ID)
			} else {
				outLog.Printf("Failed to write to node %s...\n", node.ID)
			}
		}
	}

	goVectorNetworkNodeLogger.LogLocalEvent("[Node" + ID + "] Coordinator done writing to all nodes")

	// Check if majority of writes suceeded
	threshold := Settings.MajorityThreshold
	var successRatio float32
	if len(allNodes.nodes) > 1 {
		successRatio = float32(successes) / float32(len(allNodes.nodes)-1)
	} else {
		successRatio = 1
	}
	outLog.Println("This is the write success ratio:", successRatio)

	// Update coordinator
	if successRatio > threshold || len(allNodes.nodes) == 1 {
		outLog.Println("Back up is successful! Updating coordinator KV store...")
		kvstore.Lock()
		defer kvstore.Unlock()

		key := args.Key
		value := args.Value
		kvstore.store[key] = value
		outLog.Printf("(%s, %s) successfully written to the KV store!\n", key, kvstore.store[key])
		b, err := json.MarshalIndent(kvstore.store, "", "  ")
		if err != nil {
			errLog.Println("error:", err)
		}
		outLog.Printf("Current KV mappings:\n%s\n", string(b))

		*reply = OpReply{Success: true}
	} else {
		outLog.Println("Back up failed! Aborting write...")
		*reply = OpReply{Success: false}
	}

	return nil
}

// Writing a KV pair to the network nodes
func (n KVNode) NodeWrite(args WriteRequest, reply *OpReply) error {
	goVectorNetworkNodeLogger.UnpackReceive("[Node"+ID+" ]: Receiving Write from Coordinator", args.LoggerInfo, &WriteRequest{})

	outLog.Println("Received write request from coordinator!")
	key := args.Key
	value := args.Value
	kvstore.Lock()
	defer kvstore.Unlock()

	kvstore.store[key] = value
	outLog.Printf("(%s, %s) successfully written to the KV store!\n", key, kvstore.store[key])
	b, err := json.MarshalIndent(kvstore.store, "", "  ")
	if err != nil {
		errLog.Println("error:", err)
	}
	outLog.Printf("Current KV mappings:\n%s\n", string(b))

	sendingMsg := goVectorNetworkNodeLogger.PrepareSend("[Node"+ID+"] Returning ack or nack to coordinator", OpReply{Success: true})

	*reply = OpReply{Success: true, LoggerInfo: sendingMsg}

	return nil
}

// Deleting a KV pair from the coordinator node
func (n KVNode) CoordinatorDelete(args DeleteRequest, reply *OpReply) error {
	allNodes.Lock()
	defer allNodes.Unlock()

	// Attempt delete from backup nodes
	successes := 0
	outLog.Println("Attempting to delete from back-up nodes...")
	for id, node := range allNodes.nodes {
		if id == "LoggerInfo" {
			continue
		}
		if !node.IsCoordinator {
			outLog.Printf("Deleting from node %s...\n", node.ID)

			nodeArgs := args
			nodeReply := OpReply{}
			//GoVector Logging Prepare a Message
			sendingMsg := goVectorNetworkNodeLogger.PrepareSend("[Node"+ID+"] Coordinator sending Delete to Node", nodeArgs)
			nodeArgs.LoggerInfo = sendingMsg

			err := node.NodeConn.Call("KVNode.NodeDelete", nodeArgs, &nodeReply)
			if err != nil {
				outLog.Println("Could not delete from node ", err)
			}

			goVectorNetworkNodeLogger.UnpackReceive("[Node"+ID+"] Coordinator receiving Ack/Nack from node", nodeReply.LoggerInfo, &DeleteRequest{})

			// Record successes
			if nodeReply.Success {
				successes++
				outLog.Printf("Successfully deleted from node %s!\n", node.ID)
			} else {
				outLog.Printf("Failed to delete from node %s...\n", node.ID)
			}
		}

	}

	goVectorNetworkNodeLogger.LogLocalEvent("[Node" + ID + "] Coordinator done Deleting from all nodes")

	// Check if majority of deletes suceeded
	threshold := Settings.MajorityThreshold
	var successRatio float32
	if len(allNodes.nodes) > 1 {
		successRatio = float32(successes) / float32(len(allNodes.nodes)-1)
	} else {
		successRatio = 1
	}
	outLog.Println("This is the delete success ratio:", successRatio)

	// Update coordinator
	if successRatio > threshold || len(allNodes.nodes) == 1 {
		outLog.Println("Delete from back-up is successful! Updating coordinator KV store...")
		kvstore.Lock()
		defer kvstore.Unlock()

		key := args.Key
		value := kvstore.store[key]
		if _, ok := kvstore.store[key]; ok {
			delete(kvstore.store, key)
		} else {
			return dkvlib.NonexistentKeyError(key)
		}

		outLog.Printf("(%s, %s) successfully deleted from KV store!\n", key, value)
		b, err := json.MarshalIndent(kvstore.store, "", "  ")
		if err != nil {
			errLog.Println("error:", err)
		}
		outLog.Printf("Current KV mappings:\n%s\n", string(b))

		*reply = OpReply{Success: true}
	} else {
		outLog.Println("Delete from network failed! Aborting delete...")
		*reply = OpReply{Success: false}
	}

	return nil
}

// Deleting a KV pair from the network nodes
func (n KVNode) NodeDelete(args DeleteRequest, reply *OpReply) error {
	goVectorNetworkNodeLogger.UnpackReceive("[Node"+ID+" ]: Receiving Delete from Coordinator", args.LoggerInfo, &DeleteRequest{})

	outLog.Println("Received delete request from coordinator!")
	kvstore.Lock()
	defer kvstore.Unlock()

	key := args.Key
	value := kvstore.store[key]
	if _, ok := kvstore.store[key]; ok {
		delete(kvstore.store, key)
	} else {
		outLog.Printf("Key %s does not exist in store!\n", key)
		return dkvlib.NonexistentKeyError(key)
	}
	outLog.Printf("(%s, %s) successfully deleted from KV store!\n", key, value)
	b, err := json.MarshalIndent(kvstore.store, "", "  ")
	if err != nil {
		errLog.Println("error:", err)
	}
	outLog.Printf("Current KV mappings:\n%s\n", string(b))

	sendingMsg := goVectorNetworkNodeLogger.PrepareSend("[Node"+ID+"] Returning ack or nack to coordinator", OpReply{Success: true})

	*reply = OpReply{Success: true, LoggerInfo: sendingMsg}

	return nil
}

// Notice that the coordinator has resigned
func (n KVNode) CoordinatorResign(args *FailureInfo, _unused *int) error {
	failedCoordinator := args.Failed

	if failedCoordinator.String() == Coordinator.Address.String() {
		outLog.Println("Coordinator resignation received.  Voting on new coordinator.")
		ReportCoordinatorFailure(Coordinator)
	} else {
		outLog.Println("We do not have the updated coordinator.  Updating, then voting on new coordinator.")

		allNodes.RLock()
		for id, node := range allNodes.nodes {
			if id == "LoggerInfo" {
				continue
			}
			if node.Address.String() == failedCoordinator.String() {
				Coordinator = node
				ReportCoordinatorFailure(Coordinator)
			}
		}
		allNodes.RUnlock()
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// NODE <-> NODE FUNCTION
////////////////////////////////////////////////////////////////////////////////
// Connect to given node
func ConnectNode(node *Node) error {
	outLog.Println("Attempting to connected to node...", node.ID, "[", node.Address.String(), "]")

	nodeConn, err := rpc.Dial("tcp", node.Address.String())
	if err != nil {
		if node.IsCoordinator {
			errLog.Println("Could not reach coordinator ", node.ID, "[", node.Address.String(), "]")

			info := &CoordinatorFailureInfo{
				Failed:         node.Address,
				Reporter:       LocalAddr,
				NewCoordinator: nil,
			}

			var reply int
			err = Server.Call("KVServer.ReportCoordinatorFailure", &info, &reply)
			if err != nil {
				outLog.Println("Error reporting failure of coordinator.")
			} else {
				coordinatorFailed = true
			}

		} else {
			errLog.Println("Could not reach node ", node.ID, "[", node.Address.String(), "]")
		}
		return err
	}

	node.NodeConn = nodeConn

	// Save coordinator
	if node.IsCoordinator {
		Coordinator = node
	}

	// Set up reverse connection
	args := &NodeInfo{
		Address: LocalAddr,
		ID:      ID,
	}
	var reply int
	err = nodeConn.Call("KVNode.RegisterNode", &args, &reply)
	if err != nil {
		outLog.Println("Could not initate connection with node: ", node.ID, "[", node.Address.String(), "]")
		return err
	}

	// Add this new node to node map
	allNodes.Lock()
	allNodes.nodes[node.Address.String()] = node
	allNodes.Unlock()

	outLog.Println("Successfully connected to ", node.ID, "[", node.Address.String(), "]")

	// send heartbeats
	go sendHeartBeats(node.Address.String())

	// check for timeouts
	go MonitorHeartBeats(node.Address.String())
	return nil
}

// Open reverse connection through RPC
func (n KVNode) RegisterNode(args *NodeInfo, _unused *int) error {
	addr := args.Address
	id := args.ID

	//outLog.Println("Attempting to establish return connection")
	conn, err := rpc.Dial("tcp", addr.String())

	if err != nil {
		outLog.Println("Return connection with node failed: ", id, "[", addr.String(), "]")
		return err
	}

	// Add node to node map
	allNodes.Lock()

	allNodes.nodes[addr.String()] = &Node{
		id,
		false,
		addr,
		time.Now().UnixNano(),
		conn,
	}
	allNodes.Unlock()
	outLog.Println("Return connection with node succeeded: ", id, "[", addr.String(), "]")

	go sendHeartBeats(addr.String())

	go MonitorHeartBeats(addr.String())

	return nil
}

// send heartbeats to passed node
func sendHeartBeats(addr string) error {
	args := &NodeInfo{Address: LocalAddr, ID: ID}
	var reply int
	for {
		if _, ok := allNodes.nodes[addr]; !ok {
			outLog.Println("Node not found in network. Stop sending heartbeats", addr)
			return nil
		}
		err := allNodes.nodes[addr].NodeConn.Call("KVNode.ReceiveHeartBeats", &args, &reply)
		if err != nil {
			outLog.Println("Error sending heartbeats to", allNodes.nodes[addr].ID, "[", addr, "]")

		}
		time.Sleep(time.Duration(Settings.HeartBeat) * time.Millisecond)
	}
}

// Log the most recent heartbeat received
func (n KVNode) ReceiveHeartBeats(args *NodeInfo, _unused *int) (err error) {
	addr := args.Address
	id := args.ID

	allNodes.Lock()
	defer allNodes.Unlock()

	if _, ok := allNodes.nodes[addr.String()]; !ok {
		return err
	}
	allNodes.nodes[addr.String()].RecentHeartbeat = time.Now().UnixNano()

	outLog.Println("Heartbeats received from Node ", id, "[", addr, "]")
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// MAIN, LOCAL
////////////////////////////////////////////////////////////////////////////////

func main() {
	gob.Register(&net.TCPAddr{})

	args := os.Args
	if len(args) != 2 {
		fmt.Println("Usage: go run node.go [server ip:port]")
		return
	}

	serverAddr := args[1]

	ConnectServer(serverAddr)

}

func handleErrorFatal(msg string, e error) {
	if e != nil {
		errLog.Fatalf("%s, err = %s\n", msg, e.Error())
	}
}
