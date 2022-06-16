import {promisify} from 'util';
import {RedisCluster, RedisNode} from "./api";
import calculateSlot from "cluster-key-slot";
import {randomBytes} from 'crypto';
import {hashKeyFromSlot} from "./const";
import {result} from "lodash";

import {exec as _exec} from 'child_process';
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

    let cluster: RedisCluster;

    beforeEach(async () => {
        cluster = new RedisCluster({host, port});
       // await cluster.flushdb();
    });

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

            it.skip.each([5, 10/*, 20, 40*/])("Can migrate hash key with %i000 small entries", async (n) => {
                let slot = 13000 + n;
                n *= 1000;
                let key = hashKeyFromSlot(slot);
                let source = await cluster.getSlotOwner(slot);
                let destination = await cluster.getSlotOwner(0);
                console.time("PopulateData")
                for (let i = 0; i < n; i++)
                    await cluster.hset(key, "field" + i.toString(), "value" + i.toString());
                console.timeEnd("PopulateData")

                expect(await cluster.memoryUsage(key)).toBeGreaterThan(50)
                expect(await cluster.hgetall(key)).toBeDefined()

                console.time("MigrateData")
                await cluster.migrateSlot(slot, destination);
                console.timeEnd("MigrateData")

                expect(await source.isSlotOwner(slot)).toBe(false);
                expect(await destination.isSlotOwner(slot)).toBe(true);
                expect(await destination.getKeysInSlot(slot)).toEqual([key]);
                expect(await source.getKeysInSlot(slot)).toEqual([]);
            },12000)

            it.skip("Can read key from node while it is being migrated", async () => {

            })
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

function generateRandomHexString() {
    return Math.floor(Math.random() * 200000000).toString(16);
}

async function sleep(number: number) {
    return new Promise(resolve => setTimeout(resolve, number));
}