import {promisify} from 'util';
import {exec as _exec} from 'child_process';
import {RedisCluster} from "../api";
import {DeleteLeaderboardWorker} from "./deleteLeaderboard";
const exec = promisify(_exec);


const host = "127.0.0.1";
const port = 50001;


describe("Delete Leaderboard Worker", () => {
    let cluster: RedisCluster;
    beforeAll(async () => {
        // Restart redis server
        console.log(await restartRedis());
        cluster = new RedisCluster({host, port});
    });

    afterAll(async () => {
        // Stop redis server
        console.log(await stopRedis());

        process.exit(0);
    })

    beforeEach(async () => {
        await cluster.flushdb();
        // Setup a leaderboard
        await cluster.del(`event-id`);
        await cluster.zadd(`event-id`, 1, "pid1");
        await cluster.zadd(`event-id`, 2, "pid2");

        // Setup related keys to leaderboard
        await cluster.set("{event-id}pid1", "Player 1");
        await cluster.set("{event-id}pid2", "Player 2");

        expect(await cluster.ttl("event-id")).toBe(-1);
        expect(await cluster.ttl("{event-id}pid1")).toBe(-1);
        expect(await cluster.ttl("{event-id}pid2")).toBe(-1);
    });

    afterEach(async () => {
        expect({hasFailed: await cluster.hasFailed()}).toMatchObject({hasFailed: false});
    });

    describe("Unsafe Mode > ", () => {
        it("should delete the leaderboard", async () => {
            const worker = new DeleteLeaderboardWorker(cluster, "event-id",
                {unsafe: true}
            );

            await worker.run();

            expect(await cluster.exists("event-id")).toBe(0);
        });

        it("should delete leaderboard related keys", async () => {
            const worker = new DeleteLeaderboardWorker(cluster, "event-id",
                {unsafe: true}
            );

            await worker.run();

            expect(await cluster.exists("{event-id}pid1")).toBe(0);
            expect(await cluster.exists("{event-id}pid2")).toBe(0);
        });

    });

    describe("Safe Mode > ", () => {

        it("should not delete the leaderboard but set it's expire time", async () => {
            const worker = new DeleteLeaderboardWorker(cluster, "event-id",
                {unsafe: false}
            );

            await worker.run();

            expect(await cluster.exists("event-id")).toBe(1);
            expect(await cluster.ttl("event-id")).toBeGreaterThan(0);
        });

        it("should not delete leaderboard related keys but set their expire time", async () => {
            const worker = new DeleteLeaderboardWorker(cluster, "event-id",
                {unsafe: false}
            );

            await worker.run();

            expect(await cluster.exists("{event-id}pid1")).toBe(1);
            expect(await cluster.ttl("{event-id}pid1")).toBeGreaterThan(0);

            expect(await cluster.exists("{event-id}pid2")).toBe(1);
            expect(await cluster.ttl("{event-id}pid2")).toBeGreaterThan(0);

        });

    });







});


async function restartRedis() {
    let res = await new Promise( (resolve, reject) => exec("./src/scripts/cluster.sh start").then(resolve).catch(reject))
    await sleep(1000);
    return res;
}

async function stopRedis() {
    return await new Promise( (resolve, reject) => exec("./src/scripts/cluster.sh stop").then(resolve).catch(reject))
}

async function sleep(number: number) {
    return new Promise(resolve => setTimeout(resolve, number));
}
