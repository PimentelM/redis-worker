# redis-tool
Tool to manage redis cluster


This is a work in progress, a command line interface is not built yet


## Main Features

- Fully async/await compatible partial abstraction on top of the concept of a Redis-Cluster and Redis-Node
- Simple command to perform migration
- Commands to query memory usage for nodes, slots, and individual keys
- Table to get a HashKey from a specific slot
- Command to find who is the owner of a specific slot

## Automated tests

Most important commands are fully tested using local redis cluster with 3 shards.



## Details about the "API"

I first started creating the class RedisCluster as an abstraction on top of ioredis and tedis since I intended to combine both libraries in a single toolset, but I soon realized that composing both libraries into a single class would be too much work and I decided to switch to the expose-ioredis-instance approach instead, since it can work aswell. The problem I faced with ioredis is that it does'nt seems to use proper promises and the implementation they did was'nt compatible with async await syntax. I don't know what I did but now it is working. Not sure if it was due to some change in tsconfig.json or it's just some methods that don't work as proper promises.




