import {promisify} from 'util';
import {exec as _exec} from 'child_process';
import {RedisCluster, RedisNode} from "./api";
import calculateSlot from "cluster-key-slot";

const exec = promisify(_exec);


const host = "127.0.0.1";
const port = 50001;

describe("Redis handmade API", () => {

    beforeAll(async () => {
        // Restart redis server
        console.log(await restartRedis());
    })

    afterAll(async () => {
        // Stop redis server
        console.log(await stopRedis());
    })
    
    describe("RedisCluster", ()=>{
        let cluster;

        beforeEach(async () => {
            cluster = new RedisCluster({host, port});
        });
        
        it("Can get list of Master Nodes", async () => {
            let masters = await cluster.listMasterNodes();

            expect(masters.length).toBe(3);
            expect(masters.sort(byPortNumber)).toMatchObject([
                {host, port},{host,port: port+1},{host,port: port+2}])
        })

        it("Can get owner of slot", async () => {
            let slot = 15990;
            let expectedOwnerPort = 50003;

            let owner = await cluster.getSlotOwner(slot);

            expect(owner.port).toBe(expectedOwnerPort);
        })

        it("Can get list of keys at slot", async () => {
            let slot = 15495;
            let expectedKeys= ["a", "b{a}","c{a}"];
            await cluster.cluster.set("a", "a");
            await cluster.cluster.set("b{a}", "b");
            await cluster.cluster.set("c{a}", "c");
            await cluster.cluster.set("9f3", "d");

            let keys = await cluster.getKeysInSlot(slot);

            expect(keys.sort()).toEqual(expectedKeys.sort());
        })
    })

    describe("RedisNode", () => {


        it("Can get node hash // id", async () => {
            const redis = new RedisNode(host, port);
            const hash = await redis.getHash();
            expect(hash).toMatch(/^[0-9a-f]{40}$/);
        })

        const owershipTable = [[0, true], [500, true], [5000, true], [5460, true], [6000, false], [9000, false], [11000, false], [16000, false]]
        it.each(owershipTable)("Can tell if node is owner of slot", async (slot, expected) => {
            const redis = new RedisNode(host, port);
            const isOwner = await redis.isSlotOwner(slot as number);
            expect(isOwner).toBe(expected);

        })


    })


})


async function restartRedis() {
    return await exec("./src/scripts/cluster.sh start")
}

async function stopRedis() {
    return await exec("./src/scripts/cluster.sh stop")
}

function byPortNumber(a: RedisNode, b: RedisNode) {
    return a.port - b.port;
}