import {promisify} from 'util';
import {exec as _exec} from 'child_process';
import {RedisNode} from "../api";
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








})


async function restartRedis(){
    return await exec("./src/scripts/cluster.sh start")
}

async function stopRedis(){
    return await exec("./src/scripts/cluster.sh stop")
}