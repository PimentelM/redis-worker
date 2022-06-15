# redis-tool
Tool to manage redis cluster


This is a work in progress, a command line interface is not built yet


## Main Features

- Fully async/await compatible partial abstraction on top of the concept of a Redis-Cluster and Redis-Node
- Simple command to perform migration
- Commands to query memory usage for nodes, slots, and individual keys
- Table to get a HashKey from a specific slot
- Script to start/stop local cluster with 3 shards


## Automated tests

Most important commands are fully tested using local redis cluster with 3 shards.




