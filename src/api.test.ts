import {promisify} from 'util';
import {exec as _exec} from 'child_process';
import {RedisNode} from "./api";
import calculateSlot from "cluster-key-slot";
const exec = promisify(_exec);


const host = "127.0.0.1";
const port = 50001;

describe("Redis handmade API", () => {

    beforeAll(async ()=>{
        // Restart redis server
        console.log(await restartRedis());
    })

    afterAll( async ()=>{
        // Stop redis server
        console.log(await stopRedis());
    })


    it("Should be possible to get node hash from host and port", async () => {
        const redis = new RedisNode(host, port);
        const hash = await redis.getHash();
        expect(hash).toMatch(/^[0-9a-f]{40}$/);
    })

    const owershipTable = [[0,true],[500,true],[5000,true],[5460,true],[6000,false],[9000,false],[11000,false],[16000,false]]
    it.each(owershipTable)( "Should be possible to know if a node is owner of a slot", async (slot,expected) => {
        const redis = new RedisNode(host, port);
        const isOwner = await redis.isSlotOwner(slot as number);
        expect(isOwner).toBe(expected);

    })








})


async function restartRedis(){
    return await exec("./src/scripts/cluster.sh start")
}

async function stopRedis(){
    return await exec("./src/scripts/cluster.sh stop")
}