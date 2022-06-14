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

    let cluster : RedisCluster;

    beforeEach(async () => {
        cluster = new RedisCluster({host, port});
        await cluster.flushdb();
    });
    
    describe("RedisCluster", ()=>{

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
            await cluster.set("a", "a");
            await cluster.set("b{a}", "b");
            await cluster.set("c{a}", "c");
            await cluster.set("9f3", "d");

            let keys = await cluster.getKeysInSlot(slot);

            expect(keys.sort()).toEqual(expectedKeys.sort());
        })

        it("Can migrate slot from shard to another", async () => {
            let slot = 15495;
            await cluster.set("a", "a");
            await cluster.set("b{a}", "b");
            await cluster.set("c{a}", "c");
            await cluster.set("9f3", "d");
            let destination = await cluster.getSlotOwner(0);
            let source = await cluster.getSlotOwner(slot);

            await cluster.migrateSlot(slot, destination);

            expect(await source.isSlotOwner(slot)).toBe(false);
            expect(await destination.isSlotOwner(slot)).toBe(true);
            expect(await destination.getKeysInSlot(slot)).toEqual(["a", "b{a}","c{a}"]);
            expect(await source.getKeysInSlot(slot)).toEqual([]);
            expect(await source.getKeysInSlot(calculateSlot("9f3"))).toEqual(["9f3"]);
        })

        it("Can flushdb", async () => {
            await cluster.set("a", "a");
            await cluster.set("b{a}", "b");
            let result = await cluster.flushdb();
            expect(await cluster.getKeysInSlot(calculateSlot("a"))).toEqual([]);
        })

        it("Can get keys in slot", async () => {
            await cluster.set("a", "a");
            await cluster.set("b{a}", "b");
            await cluster.set("c{a}", "c");
            expect((await cluster.getKeysInSlot(calculateSlot("a"))).sort()).toEqual(["a", "b{a}","c{a}"]);
        })

        it("Can get memory usage of key", async () => {
            await cluster.set("a", "a");

            expect(await cluster.memoryUsage("a")).toBeGreaterThan(50);
        })
    })

    describe("Live Resharding", ()=>{

        it("Can migrate slots containing 5MB of data", async () => {


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

function generateRandomHexString(){
    return (Math.random()*2147483648).toString(16);
}