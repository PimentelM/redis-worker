import {Cluster} from "ioredis";
import {Tedis} from "tedis";
import {keyFromSlot} from "./const";


export class RedisCluster {
    public cluster : Cluster;
    private node : RedisNode;

    constructor(private host : {host: string, port: number},) {
        this.cluster = new Cluster([this.host]);
        this.node = new RedisNode(this.host.host, this.host.port);
    }

    async listMasterNodes() : Promise<RedisNode[]>{

        let raw =  await this.node.command('cluster', 'nodes');

        let nodes = raw.split("\n").filter(x=>x.includes("master") && (/^.*\d$/).test(x)).map(x=>x.split(" ")[1].split("@")[0].split(":")).map(x=>new RedisNode(x[0], parseInt(x[1])));

        return nodes;
    }

    async listSlotsBySize(){}

    async getKeysInSlot(slot: number): Promise<string[]>{
        return []
    }

    async getSlotOwner(slot:number): Promise<RedisNode>{
       for(let node of await this.listMasterNodes()){
           if(await node.isSlotOwner(slot)){
               return node;
           }
       }

       throw new Error(`No node found for slot ${slot}`);
    }

    async migrateSlot(slot: number, destination: RedisNode, timeout = 2000){

        // Get all keys from slot
        let keys = await this.getKeysInSlot(slot);

        // Get sourceNode info
        let source: RedisNode = await this.getSlotOwner(slot);

        // Get hashes
        let sourceHash = await source.getHash();
        let destinationHash = await destination.getHash();

        // Set destinationNode state as IMPORTING
        await destination.command(...`cluster setslot ${slot} importing ${sourceHash}`.split(""))

        // Set sourceNode state as MIGRATING
        await source.command(...`cluster setslot ${slot} migrating ${destinationHash}`.split(""))

        // Perform migration
        await source.command('migrate', destination.host, destination.port, '', 0, timeout, "keys", ...keys);

        // Restore nodes to original state
        await source.command(...`cluster setslot ${slot} node ${destinationHash}`.split(""))
        await destination.command(...`cluster setslot ${slot} node ${destinationHash}`.split(""))


    }

}


export class RedisNode {
    host: string;
    port: number;

    private tedis: Tedis;

    constructor(host: string, port: number){
        this.host = host;
        this.port = port;

        this.tedis = new Tedis({host, port});
    }

    async command(...args: Array<string | number>): Promise<any>{
        return this.tedis.command(...args);
    }

    async getHash(){
        let raw = await this.tedis.command("cluster", "nodes")

        let myLineRaw = raw.split("\n").filter(x=>x.includes(`myself,master`))[0]

        return myLineRaw.split(" ")[0];
    }

    async isSlotOwner(slot: number): Promise<boolean>{
        return await this.tedis.get(keyFromSlot(slot)).then(x => true).catch(x => false);
    }

    async getKeysInSlot(slot: number): Promise<string[]>{
        return []
    }

}