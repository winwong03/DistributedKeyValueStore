# Regina and The Plastics KV DB
A simple distributed key-value store project for UBC CPSC 416 2017W2

## Requirements
This project is built upon and intended to be run on Golang Ver. 1.9.2

## Running This System
The following is to start up the KV server for tracking network nodes
server.go: `server.go -c config.json`

The following is to start up a network node for the KV network
node.go: `go run node.go [server ip:port]`

The following is to utilize a basic test client to interact with the KV network
client.go: `go run client.go [server ip:port]`

The following is to utilize a client with CLI input commands to interact with the KV network
client_proto.go `go run client_proto.go [server ip:port] [cmd]`

[cmd] is one of more of the following:
| Command | Description |
| R,<key> | Reads a value associated with <key> from the key-value store. |
| W,<key>,<value> | Writes a value <value> with the key <key> to the key-value store. | 
| D,<key> | Deletes a value associated with <key> from the key-value store. |

Note that the above commands are case sensitive

## System Overview
The Regina and The Plastics Key-Value Database is a distributed key-value store that is to be hosted on multiple instances of or separate datastores. This solution solves the problem of having a single point of failure and high availablitly. Within the system, one node, known as the coordinator, shall act as master node that hosts all user-input data. All other nodes of the network act as primary replicants.

## System Design
The Regina and The Plastics KV DB is comprised of three different types of network nodes: the server, KV network nodes, and the application client.  The server allows new KV network nodes to enter the network and is also responsible for tracking of the network state. Every KV network node is aware of every other KV network node through a heartbeat connection. The network nodes are comprised of replicant KV stores and a coordinator node. The coordinator is responsible for updating and retrieving data from the replicant stores and is the node that the client interfaces with when making requests to the network.

### Server
The server is responsible for the tracking and registration of new KV network nodes. The only connection that the server maintains to the network is to the coordinator node. Upon failure of the coordinator, all nodes will elect a new coordinator, and the server shall confirm the appointed node. The first node to register with the server is automatically appointed the coordinator node. Through the Coordinator node, the server is periodically updated with the statuses of all of the nodes.

### Node
#### Coordinator Node
The coordinator node acts as a proxy between clients and replicant network nodes. This node handles various requests from clients and maintains a connection to the server at all times.

#### Replicant Node
The first node to connect to the server will automatically be appointed coordinator. All other nodes that join thereafter will act as replicant nodes to the KV store in the coordinator. A replicant network node, however, may be elected as a coordinator should the current coordinator fail. All replicant nodes maintain a heartbeat connection between themselves and the coordinator.

### Application Client
An application client is comprised of two pieces: a DKV API library, and a client script. The client script utilizes the API to make READ, WRITE, and DELETE requests to the KV network. The application initially connects to the server in order to discover who the current coordinator is before connecting to the coordinator. 

#### DKV API
The DKV API houses the necessary functionality to interact with the KV network. Through this library, the client is able to join the KV network, and initiate READ, WRITE, and DELETE requests.

#### Client
The client is what allows the user to interface with the KV netork. With the client, a user will be able to make requests to the network through CLI commands.

## Azure Deployment
This project has been designed to be deployed on Microsoft Azure, although it may be run on a local set-up for use and testing.# DistributedKeyValueStore
