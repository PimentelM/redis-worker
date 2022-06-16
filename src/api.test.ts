import {promisify} from 'util';
import {RedisCluster, RedisNode} from "./api";
import calculateSlot from "cluster-key-slot";
import {randomBytes} from 'crypto';
import {hashKeyFromSlot} from "./const";
import _, {result} from "lodash";

import {exec as _exec} from 'child_process';
const exec = promisify(_exec);

import {readFile as _readFile} from 'fs';
const readFile = promisify(_readFile);

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

    let cluster: RedisCluster;

    beforeEach(async () => {
        cluster = new RedisCluster({host, port});
       // await cluster.flushdb();
    });

    // afterEach(async () => {
    //   console.log((await cluster.listMasterNodes()).map(x=>x.port).join(","));
    // })

    describe("RedisCluster", () => {

        it("Can get list of Master Nodes", async () => {
            let masters = await cluster.listMasterNodes();

            expect(masters.length).toBe(3);
            expect(masters.sort(byPortNumber)).toMatchObject([
                {host, port}, {host, port: port + 1}, {host, port: port + 2}])
        })

        it("Can get owner of slot", async () => {
            let slot = 15990;
            let expectedOwnerPort = 50003;

            let owner = await cluster.getSlotOwner(slot);

            expect(owner.port).toBe(expectedOwnerPort);
        })

        it("Can dump value", async () => {
            let key = "dumpKey";
            let value = "dumpValue";
            await cluster.set(key, value);

            let dump = await cluster.dump(key);

            expect(dump).toBeDefined();
        })

        it("Can restore value", async () => {
            let key = "restoreKey";
            let dump = Buffer.from("000964756d7056616c75650a0082aa58764e454928", "hex");

            await cluster.restore(key, dump);

            let restoredValue = await cluster.get(key);
            expect(restoredValue).toBe("dumpValue");
            expect(await cluster.clusterNodesRaw()).not.toContain("fail")
        })

        it("Can restore really big dump", async () => {
            let key = `restoreKey${generateRandomHexString(16)}`;
            let dump = await readFile("./orderedSetDump")

            await cluster.restore(key, dump);

            let restoredValue = await cluster.zrange(key,0,2);
            expect(restoredValue).toHaveLength(3);
            let clusterNodesRaw = await cluster.clusterNodesRaw();
            expect(clusterNodesRaw.includes("fail")).toBeFalsy();

        },20000)



        it("Can get list of keys at slot", async () => {
            let slot = 15495;
            let expectedKeys = ["a", "b{a}", "c{a}"];
            await cluster.set("a", "a");
            await cluster.set("b{a}", "b");
            await cluster.set("c{a}", "c");
            await cluster.set("9f3", "d");

            let keys = await cluster.getKeysInSlot(slot);

            expect(keys.sort()).toEqual(expectedKeys.sort());
        })

        it("Can get all keys in cluster", async () => {
            let key1 = `all{${hashKeyFromSlot(1)}}`;
            let key2 = `all{${hashKeyFromSlot(7500)}}`;
            let key3 = `all{${hashKeyFromSlot(13000)}}`;
            await cluster.set(key1, "a");
            await cluster.set(key2, "b");
            await cluster.set(key3, "c");

            let keys = await cluster.getAllKeys();

            expect(keys).toContain(key1);
            expect(keys).toContain(key2);
            expect(keys).toContain(key3);
        })

        describe("Live Resharding", () => {
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
                expect(await destination.getKeysInSlot(slot)).toEqual(["a", "b{a}", "c{a}"]);
                expect(await source.getKeysInSlot(slot)).toEqual([]);
                expect(await source.getKeysInSlot(calculateSlot("9f3"))).toEqual(["9f3"]);
            })

            it("Memory usage should be lesser after migration", async () => {
                let dataSize = 1024 * 1024 * 20
                let slot = 16100;
                let key = `lesserMemory{${hashKeyFromSlot(slot)}}`;
                let value = 'x'.repeat(dataSize);
                let source = await cluster.getSlotOwner(slot);
                let destination = await cluster.getSlotOwner(0);
                await cluster.set(key, value);
                let {usedMemory: usedMemoryBeforeMigration } = await source.info();

                await cluster.migrateSlot(slot, destination);

                let {usedMemory: usedMemoryAfterMigration } = await source.info();
                let difference = usedMemoryBeforeMigration - usedMemoryAfterMigration;
                expect(usedMemoryAfterMigration).toBeLessThan(usedMemoryBeforeMigration);
                expect(difference).toBeGreaterThan(dataSize * 0.9);
            })

            it.each([50, 100, 200])("Can migrate simple key with %iMB of data", async (MB) => {
                let slot = 15000 + MB;
                let key = hashKeyFromSlot(slot);
                let source = await cluster.getSlotOwner(slot);
                let destination = await cluster.getSlotOwner(0);
                console.time("PopulateData")
                let data = 'x'.repeat(MB * 1024 * 1024);
                await cluster.set(key, data);
                console.timeEnd("PopulateData")

                console.time("MigrateData")
                await cluster.migrateSlot(slot, destination);
                console.timeEnd("MigrateData")

                expect(await source.isSlotOwner(slot)).toBe(false);
                expect(await destination.isSlotOwner(slot)).toBe(true);
                expect(await destination.getKeysInSlot(slot)).toEqual([key]);
                expect(await source.getKeysInSlot(slot)).toEqual([]);

            }, 24000)


            it.skip("Can read key from slot while it is being migrated", async () => {
                let slot = 12121;
                let key = hashKeyFromSlot(slot);
                let dump = await readFile(`./orderedSetdump`);
                let destination = await cluster.getSlotOwner(0);
                await cluster.restore(slot, dump);

                let masters = _.sortBy(await cluster.listMasterNodes(),x=>x.port);

                let hashes = await Promise.all(masters.map(async (master) => {
                    return await master.getHash();
                }))

                let keys = [99, 6765, 16383].map(x=>`unixistentkeys{${hashKeyFromSlot(x)}}`)

                let slotmap = await new Promise((resolve, reject) => cluster[`ioredis`].cluster("SLOTS").then(resolve).catch(reject));

                let clusterNodes = await new Promise((resolve, reject) => cluster[`ioredis`].cluster("NODES").then(resolve).catch(reject));

                let result1 = await masters.shift()!.command("GET",keys.shift() as string).catch(err=>{
                    console.log(err);
                    return err;
                })
                let result2 = await masters.shift()!.command("GET",keys.shift() as string).catch(err=>{
                    console.log(err);
                    return err;
                })
                let result3 = await masters.shift()!.command("GET",keys.shift() as string).catch(err=>{
                    console.log(err);
                    return err;
                })








                let promise = cluster.migrateSlot(slot,destination).then(()=>{
                    return true;
                })

                // Should be able to read range from slot
                let range = await cluster.zrange(key, 0, 3);
                expect(range).toHaveLength(3);
                expect(range).toEqual(["a", "b", "c"]);
                expect(promiseState(promise)).toBe("pending");
            }, 900000)


        })

        it.skip("Can flushdb", async () => {
            await cluster.set("b", "a");
            await cluster.set("b{b}", "b");

            let result = await cluster.flushdb();

            expect(result).toBe("OK");
            expect(await cluster.getKeysInSlot(calculateSlot("b"))).toEqual([]);
        })

        it("Can get memory usage of key", async () => {
            await cluster.set("a", "a");

            expect(await cluster.memoryUsage("a")).toBeGreaterThan(50);
        })
    })

    describe("RedisNode", () => {


        it("Can get node hash // id", async () => {
            const redis = new RedisNode(host, port);
            const hash = await redis.getHash();
            expect(hash).toMatch(/^[0-9a-f]{40}$/);
        })

        it("Info should contain memory usage", async () => {
          let info = await new RedisNode(host, port).info();

            expect(info.usedMemory).toBeDefined();
        })

        it("Can get memory usage from key", async () => {
          let node = new RedisNode(host, port);
          let key = hashKeyFromSlot(750);
          await node.command("set", key, "a");

          let result = await node.memoryUsage(key);

          expect(result).toBeGreaterThan(50);
        })

        it("Can get all keys with memory usage", async () => {
            let node = new RedisNode(host, port);
            let key = hashKeyFromSlot(751);
            await node.command("set", key, "a");

            let result = await node.getAllKeysWithMemoryUsage();

            expect(result.length).toBeGreaterThan(0);
            expect(result.find(x=>x.key === key)).toBeDefined();
            expect(result.find(x=>x.key === key)!.memoryUsage).toBeGreaterThan(50);

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
     let res = await new Promise( (resolve, reject) => exec("./src/scripts/cluster.sh start").then(resolve).catch(reject))
    await sleep(1000);
    return res;
}

async function stopRedis() {
    return await new Promise( (resolve, reject) => exec("./src/scripts/cluster.sh stop").then(resolve).catch(reject))
}

function byPortNumber(a: RedisNode, b: RedisNode) {
    return a.port - b.port;
}

function generateRandomHexString(size) {
    return randomBytes(size).toString("hex");
}

async function sleep(number: number) {
    return new Promise(resolve => setTimeout(resolve, number));
}

function promiseState(p) {
    const t = {};
    return Promise.race([p, t])
        .then(v => (v === t)? "pending" : "fulfilled", () => "rejected");
}
