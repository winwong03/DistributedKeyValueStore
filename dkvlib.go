package dkvlib

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"regexp"
)

///////////////////////////////////////////////////////////////////////////
// ERROR DEFINITIONS
///////////////////////////////////////////////////////////////////////////
type InvalidKeyCharError string

func (e InvalidKeyCharError) Error() string {
	return fmt.Sprintf("DKV: Invalid character in key [%s]", string(e))
}

type InvalidValueCharError string

func (e InvalidValueCharError) Error() string {
	return fmt.Sprintf("DKV: Invalid character in value [%s]", string(e))
}

type KeyTooLongError string

func (e KeyTooLongError) Error() string {
	return fmt.Sprintf("DKV: Key is above character limit [%s]", string(e))
}

type ValueTooLongError string

func (e ValueTooLongError) Error() string {
	return fmt.Sprintf("DKV: Value is above character limit [%s]", string(e))
}

type CoordinatorWriteError string

func (e CoordinatorWriteError) Error() string {
	return fmt.Sprintf("DKV: Could not write to the coordinator node. Write failed [%s]", string(e))
}

type MajorityOpError string

func (e MajorityOpError) Error() string {
	return fmt.Sprintf("DKV: Could not complete operation on a majority of network nodes. Operation failed [%s]", string(e))
}

type NonexistentKeyError string

func (e NonexistentKeyError) Error() string {
	return fmt.Sprintf("DKV: The desired key does not exist [%s]", string(e))
}

type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("DKV: Cannot connect to [%s]", string(e))
}

///////////////////////////////////////////////////////////////////////////
// TYPES, VARIABLES, CONSTANTS
///////////////////////////////////////////////////////////////////////////

var (
	errLog *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	outLog *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
)

// Represent a Coordinator node
type CNodeConn interface {
	Read(key string) (string, error)
	Write(key, value string) error
	Delete(key string) error
	SendHeartbeat() (int64, error)
}

type CNode struct {
	coordinatorAddr string
	Coordinator     *rpc.Client
	connected       bool
}

////////////////////////////////////////////////////////////////////////////////
// TYPES, STRUCTURES
////////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////
// CLIENT-COORDINATOR FUNCTIONS
///////////////////////////////////////////////////////////////////////////

// Connect to the coordinator
func OpenCoordinatorConn(coordinatorAddr string) (cNodeConn CNodeConn, err error) {
	// Connect to coordinatorNode
	coordinator, err := rpc.Dial("tcp", coordinatorAddr)
	if err != nil {
		outLog.Println("Could not connect to coordinator.", errors.New("Coordinator Disconnected"))
		return nil, errors.New("Coordinator Disconnected")
	}

	// Create coord node
	cNodeConn = &CNode{coordinatorAddr, coordinator, true}

	return cNodeConn, nil
}

// Get value of key
func (c CNode) Read(key string) (string, error) {

	args := &ReadRequest{Key: key}
	reply := ReadReply{}

	outLog.Printf("Sending read to coordinator")
	err := c.Coordinator.Call("KVNode.CoordinatorRead", args, &reply)
	if err != nil {
		outLog.Println("Could not connect to coordinator: ", err)
		return "", err
	}
	if !reply.Success {
		return "", errors.New("Error retrieving key")
	}

	return reply.Value, nil
}

// Write value to key
func (c CNode) Write(key, value string) error {
	// Check for valid characters
	regex, err := regexp.Compile("^[a-zA-Z0-9]+$")
	if !regex.MatchString(key) {
		return InvalidKeyCharError(key)
	}
	if !regex.MatchString(value) {
		return InvalidValueCharError(value)
	}

	outLog.Printf("WRITING KEY: %s with VALUE: %s\n", key, value)

	args := &WriteRequest{Key: key, Value: value}
	reply := OpReply{}

	outLog.Printf("Sending write to coordinator")
	err = c.Coordinator.Call("KVNode.CoordinatorWrite", args, &reply)
	if err != nil {
		outLog.Println("Could not connect to coordinator: ", err)
		return err
	}

	// Check if write was successful
	if reply.Success {
		outLog.Println("Successfully completed write to coordinator")
	} else {
		outLog.Println("Failed to write to coordinator...")
		return MajorityOpError("Failed to write to majority of nodes")
	}

	return nil
}

// Delete key-value pair
func (c CNode) Delete(key string) error {
	outLog.Printf("DELETING KEY: %s\n", key)

	args := &DeleteRequest{Key: key}
	reply := OpReply{}

	outLog.Printf("Sending delete to coordinator")
	err := c.Coordinator.Call("KVNode.CoordinatorDelete", args, &reply)
	if err != nil {
		return err
	}

	// Check if delete was ssuccessful
	if reply.Success {
		outLog.Println("Successfully completed delete")
	} else {
		outLog.Println("Failed to delete from coordinator...")
		return MajorityOpError("Failed to delete to majority of nodes")
	}

	return nil
}

// Check that RPC connection is still alive.
func (c CNode) SendHeartbeat() (int64, error) {
	var args int
	var reply int64
	err := c.Coordinator.Call("KVNode.SendHeartbeat", &args, &reply)
	return reply, err
}
