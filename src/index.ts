import {RedisCluster, RedisNode} from "./api";
import {hashKeyFromSlot} from "./const";
import {randomBytes} from "crypto";
import {exec as _exec} from "child_process";
import {promisify} from "util";
const exec = promisify(_exec);
import calculateSlot from "cluster-key-slot";


import {readFile as _readFile, writeFileSync} from "fs";
import {DownTimeCheckerWorker} from "./downTimeChecker";
const readFile = promisify(_readFile)

let run = async () => {
    let cluster = new RedisCluster({host: "test-cluster-2.pi8pe7.clustercfg.usw2.cache.amazonaws.com", port: 6379});
    let downtimeWatcher = new DownTimeCheckerWorker(
        cluster,
        {},
        "22-07-06:redis-tool-test-16:15",
        1000 * 60 * 60,
        1000,
        50
    )

    downtimeWatcher.start();

    process.on('SIGINT', async function() {
        await downtimeWatcher.stop();
        process.exit();
    });

}

run().then(()=>console.log(`done`)).catch(err=>{
    console.log(`Unhandled error:`)
    console.log(err)
});




async function restartRedis() {
    await new Promise( (resolve, reject) => exec("./src/scripts/cluster.sh stop").then(resolve).catch(reject))
    await sleep(500);
    let res = await new Promise( (resolve, reject) => exec("./src/scripts/cluster.sh start").then(resolve).catch(reject))
    await sleep(10000);
    await exec("redis-cli -h 127.0.0.1 -p 50003 get a").catch(err=>{
        console.log(`Error while restarting redis`)
        console.log(err)
    })
    return res;
}

async function sleep(number: number) {
    return new Promise(resolve => setTimeout(resolve, number));
}




