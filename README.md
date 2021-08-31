# Distributed Key-Value Store

## Background
This project is a distributed, fault-tolerant key-value store written in Golang using remote procedure calls (RPC) for communication. The database can concurrently service multiple clients and stores data across multiple nodes for high availability. As long as at least one node is online, the database can continue servicing incoming requests. Storage nodes persist data to disk, allowing the nodes to recover data after a restart. If this data is stale, the storage node must catch up and become consistent with active storage nodes before rejoining the system.

This project was the final assignment for the 2020W session of the Distributed Systems course at UBC (CPSC 416).

## High-Level Architecture
### Topology
There are three types of nodes in this system: client, storage, and frontend.

The system is designed using a centralized architecture, with a single frontend node acting as a coordinator. Client nodes connect to the frontend node and submit GET or PUT requests. The frontend is responsible for relaying these requests across all the connected storage nodes. GET requests only need to request data from a single storage node, but PUT requests must eventually update all the connected storage nodes to maintain consistency.

### Consistency Semantics
This system guarantees monotonic reads and monotonic writes. That is, for any GET request to some key X, a successive GET request to X will return the same or a more recent value. For any two successive PUT requests to some key X with values V1 and V2, the system will observe V1 before V2.

Note that the consistency semantics are only guaranteed per client in this implementation, not across clients.

### Data Persistence
On each storage node, data is persisted to disk using a simple write-ahead logging protocol. The data is stored in JSON format, and must be written to disk before a PUT request can complete successfully.

Upon restarting after a failure, a storage node can recover data by reading the JSON log file from disk and applying each operation. If it is the only connected storage node, it can immediately begin servicing new requests from the frontend node. If other storage nodes are connected, the restarted storage node must "catch up" to the active storage nodes and reach a consistent state before rejoining the system. In this case, the rejoining storage node will first contact the frontend node. The frontend node will contact other active storage nodes and obtain the most up-to-date state to replay on the rejoining node.

## Getting Started
A Makefile is provided to easily build all the required components. Run `make all` to compile the executables for each type of node.

Start the frontend (coordinator) node by running `./frontend`.

Start multiple storage nodes by running `./storage --id <name> --listen <ip:port>` for each storage node you wish to run. Be sure to pass in a unique name and an IP address/port number to listen on.

Connect and run clients by running `./client --id <name>` for each client. Be sure to pass in a unique name for each client.

Further configuration can be applied by altering the config files in the `/config` directory.
