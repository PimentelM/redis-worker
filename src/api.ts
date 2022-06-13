import IORedis, {Cluster} from "ioredis";
import {Tedis} from "tedis";


export class RedisApi {
    public cluster : Cluster;

    constructor(private host : {host: string, port: number},) {
        this.cluster = new Cluster([this.host]);
    }

    async listMasterNodes() : Promise<RedisNode[]>{
        return []
    }

    async listSlotsBySize(){}

    async getKeysInSlot(slot: number): Promise<string[]>{
        return []
    }

    async getSlotOwner(slot:number): Promise<RedisNode>{
       return new RedisNode(``,0)
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
        let rawSlotTable = await this.command('cluster', 'slots');

        let rawRanges = rawSlotTable.split(/^(\d)+\)/)


        return false

    }

    async getKeysInSlot(slot: number): Promise<string[]>{
        return []
    }

}