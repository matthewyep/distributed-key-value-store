# Distributed Key-Value Store

## Background
This project is a distributed, fault-tolerant key-value store written in Golang using remote procedure calls (RPC) for communication. The database can concurrently service multiple clients and stores data across multiple nodes for high availability. As long as at least one node is online, the database can continue servicing incoming requests. Storage nodes persist data to disk, allowing the nodes to recover data after a restart. If this data is stale, the storage node must catch up and become consistent with active storage nodes before rejoining the system.

This project was the final assignment for the 2020W session of the Distributed Systems course at UBC (CPSC 416).
