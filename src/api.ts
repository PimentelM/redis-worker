import {Cluster} from "ioredis";
import {Tedis} from "tedis";
import {hashKeyFromSlot} from "./const";
import * as Buffer from "buffer";
import _ from "lodash";
import calculateSlot from "cluster-key-slot";
import {randomBytes} from "crypto";


export class RedisCluster {
    private ioredis: Cluster;
    private node: RedisNode;
    private nodeCache: { [key: string]: RedisNode } = {};

    private _hostInfo;

    constructor(private host: { host: string, port: number },) {
        this._hostInfo = host;
        this.ioredis = new Cluster([this.host]);
        this.node = new RedisNode(this.host.host, this.host.port);
    }

    async clusterNodesRaw(){
        let result = await new Promise((resolve, reject) => this.ioredis.cluster("NODES").then(resolve).catch(reject));
        return result as any as string
    }

    getNode(host: string, port: number){
        if (!this.nodeCache[`${host}:${port}`]) {
            this.nodeCache[`${host}:${port}`] = new RedisNode(host, port);
        }
        return this.nodeCache[`${host}:${port}`];
    }

    async listMasterNodes(): Promise<RedisNode[]> {
        let raw = await this.clusterNodesRaw();
        let nodes = (raw as string).split("\n").filter(x => x.includes("master") && (/^.*\d$/).test(x)).map(x => x.split(" ")[1].split("@")[0].split(":")).map(x => this.getNode(x[0], parseInt(x[1])));

        return nodes;
    }

    async restore(key, serializedData: string | Buffer) {
        let result = await new Promise((resolve, reject) => this.ioredis.restore(key, 0, serializedData).then(resolve).catch(reject))
        //let slotOwner = await this.getSlotOwner(calculateSlot(key));

        //let result = await slotOwner.command("RESTORE",key,0, serializedData.toString());

        return result;
    }

    async dump(key): Promise<Buffer>{
        return new Promise((resolve, reject) => this.ioredis.dumpBuffer(key).then(resolve).catch(reject));
    }

    async getKeysInSlot(slot: number): Promise<string[]> {
        let owner = await this.getSlotOwner(slot);

        return owner.getKeysInSlot(slot);
    }

    async set(key: string, value: string | Buffer | number) {
        return new Promise((resolve, reject) => this.ioredis.set(key, value).then(resolve).catch(reject));
    }

    async get(key: string) {
        return new Promise((resolve, reject) => this.ioredis.get(key).then(resolve).catch(reject));
    }

    async hset(key: string, field: string, value: string | Buffer | number) {
        return new Promise((resolve, reject) => this.ioredis.hset(key, field, value).then(resolve).catch(reject));
    }

    async zadd(key: string, score: number, value: string, ...args: (string | number)[]) {
        return new Promise((resolve, reject) => this.ioredis.zadd(key, score, value, ...args).then(resolve).catch(reject));
    }

    async zrange(key: string, min: number, max: number, withscores?: "WITHSCORES") {
        if (withscores)
            return new Promise((resolve, reject) => this.ioredis.zrange(key, min, max, "WITHSCORES").then(resolve).catch(reject));
        return new Promise((resolve, reject) => this.ioredis.zrange(key, min, max).then(resolve).catch(reject));
    }

    async sadd(key: string, value: string, ...args: (string | number)[]) {
        return new Promise((resolve, reject) => this.ioredis.sadd(key, value, ...args).then(resolve).catch(reject));
    }

    async hget(key: string, field: string) {
        return new Promise((resolve, reject) => this.ioredis.hget(key, field).then(resolve).catch(reject));
    }

    async hgetall(key) {
        return new Promise((resolve, reject) => this.ioredis.hgetall(key).then(resolve).catch(reject));
    }

    async getAllKeys() {
        let nodes = await this.listMasterNodes();
        return (await Promise.all(nodes.map(node => node.getAllKeys()))).flat();
    }

    async getSlotOwner(slot: number): Promise<RedisNode> {
        let nodesList = await this.listMasterNodes()
        for (let node of nodesList) {
            if (await node.isSlotOwner(slot)) {
                return node;
            }
        }

        throw new Error(`No node found for slot ${slot}`);
    }

    async migrateSlot(slot: number, destination: RedisNode, timeout = 2000) {
        let error;

        // Get all keys from slot
        let keys = await this.getKeysInSlot(slot);

        // Get sourceNode info
        let source: RedisNode = await this.getSlotOwner(slot);

        // Get hashes
        let sourceHash = await source.getHash();
        let destinationHash = await destination.getHash();

        try {
            // Set destinationNode state as IMPORTING
            await destination.command(...`cluster setslot ${slot} importing ${sourceHash}`.split(" "))

            // Set sourceNode state as MIGRATING
            await source.command(...`cluster setslot ${slot} migrating ${destinationHash}`.split(" "))

            // Perform migration

            await source.command('migrate', destination.host, destination.port, '', 0, timeout, "keys", ...keys);
        } catch (e) {
            console.log(e);
            error = e;
        } finally {
            // Restore nodes to original state
            await source.command(...`cluster setslot ${slot} node ${destinationHash}`.split(" "))
            await destination.command(...`cluster setslot ${slot} node ${destinationHash}`.split(" "))
            this.ioredis = new Cluster(this._hostInfo);
        }

        if(error) throw error;
    }

    async flushdb() {
        await new Promise((resolve, reject) => this.ioredis.flushall().then(resolve).catch(reject));
        return new Promise((resolve, reject) => this.ioredis.flushdb("SYNC").then(resolve).catch(reject));
    }

    async memoryUsage(key: string) {
        return new Promise((resolve, reject) => this.ioredis.memory("USAGE", key).then(resolve).catch(reject));
    }
}


export class RedisNode {
    host: string;
    port: number;

    private tedis: Tedis;

    constructor(host: string, port: number) {
        this.host = host;
        this.port = port;

        this.tedis = new Tedis({host, port});
    }

    async info(): Promise<NodeInfo & any> {
        let arrayOfData = await this.tedis.command("info");
        let result = {};

        for (let line of arrayOfData) {
            if (line.includes(":")) {
                let [key, value] = line.split(":");
                if (/^\d*$/.test(value)) {
                    value = parseInt(value);
                }
                result[_.camelCase(key)] = value;
            }
        }

        return result as NodeInfo;

    }

    async command(...args: Array<string | number>): Promise<any> {
        return this.tedis.command(...args);
    }

    async getHash() {
        let raw = await this.tedis.command("cluster", "nodes")

        let myLineRaw = raw.split("\n").filter(x => x.includes(`myself,master`))[0]

        return myLineRaw.split(" ")[0];
    }

    async isSlotOwner(slot: number): Promise<boolean> {
        return await this.tedis.get(`this_key_should_never_exist{${hashKeyFromSlot(slot)}}${randomBytes(8).toString('hex')}`).then(x => true)
            .catch(err => {
                if(err.toString().includes("MOVED")){
                    return false;
                }
                throw(err);
            });
    }

    async getKeysInSlot(slot: number): Promise<string[]> {
        return await this.tedis.command('cluster', 'getkeysinslot', slot, '4294967295');
    }

    async getAllKeys() {
        return await this.tedis.keys("*");
    }

    async memoryUsage(key: string) {
        return await this.tedis.command("MEMORY", "USAGE", key);
    }

    async getAllKeysWithMemoryUsage(): Promise<{ key: string, memoryUsage: number, slot: number }[]> {
        let keys = await this.getAllKeys();
        let results = await Promise.all(keys.map(key => this.memoryUsage(key)));
        let result = keys.map((key, index) => ({key, memoryUsage: results[index], slot: calculateSlot(key)}));
        return _.sortBy(result, x => x.memoryUsage);
    }
}

interface NodeInfo {
    usedMemory: number;
}