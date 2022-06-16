import {RedisCluster, RedisNode} from "./api";
import {hashKeyFromSlot} from "./const";
import {randomBytes} from "crypto";
import {exec as _exec} from "child_process";
import {promisify} from "util";
const exec = promisify(_exec);

import {readFile as _readFile} from "fs";
const readFile = promisify(_readFile)

let run = async () => {

    //await restartRedis().then(x=>console.log(x));

    let _tedis = new RedisNode("127.0.0.1", 50001);
    let _tedisHash = await _tedis.getHash()
    console.log(`Connected to: `+ _tedisHash);


    let cluster = new RedisCluster({host: "127.0.0.1", port: 50001});

    let cslots = await cluster[`ioredis`].cluster("SLOTS")
    console.log(cslots)




    let unnaffectedSlot = 1;
    let unnaffectedKey = `u{${hashKeyFromSlot(unnaffectedSlot)}}`;
    let unnaffectedKey2 = `u2{${hashKeyFromSlot(unnaffectedSlot)}}`;
    await cluster.set(unnaffectedKey, "X");


    // Master node hashes:
    let masterNodes = await cluster.listMasterNodes();
    for (const node of masterNodes) {
        console.log(`Node ${node.port}: ${await node.getHash()}`)
    }

    let key = `zset{${hashKeyFromSlot(0)}}`;
    let slot = 0

    // Read file orderedsetDump file
    let buffer = await readFile("./orderedsetDump");

    // Restore the key and data
    await cluster.restore(key, buffer).then(()=> console.log(`Restored OK`))



    //Start migration from one cluster to another
    let source = await cluster.getSlotOwner(0);
    let destination = await cluster.getSlotOwner(11000);

    console.log(`Source: ${source.host}:${source.port}`);
    console.log(`Destination: ${destination.host}:${destination.port}`);
    console.log(`Key: ${key}`);
    console.log(`Slot: ${slot}`);
    console.log(`Starting Migration...`)

    console.time("Async Migration");
    cluster.migrateSlot(slot,destination).then(() => {
        console.log(`Migration Complete`);
        console.timeEnd("Async Migration");
    }).catch(err=>{
        console.timeEnd("Async Migration")
        console.log(`Error while migrating`)
        console.log(err)
    })

    console.log(`Proceding to queries and inserts...`)


    let firstQueryResult : any = await cluster.zrange(key,0,3).catch((err)=>{
        console.log(`Error query1:`)
        console.log(err)
    })
    console.log(`Query1 result: ${firstQueryResult} {len: ${firstQueryResult.length})`)

    let addResult =  await cluster.zadd(key, 0, "This should be in top 3").catch((err)=>{
        console.log(`Error insert1:`)
        console.log(err)
    })
    console.log(`Insert1 result: ${addResult}`)

    let secondQueryResult = await cluster.zrange(key,0,3).catch((err)=>{
        console.log(`Error query2:`)
        console.log(err)
    })
    console.log(`Query2 result: ${secondQueryResult}`)

    let q3 = await cluster.get(unnaffectedKey).catch((err)=>{
        console.log(`Error query3:`)
        console.log(err)
    })
    console.log(`Query3 result: ${q3}`)

    let i2 = await cluster.set(unnaffectedKey2, "X").catch((err)=>{
        console.log(`Error insert2:`)
        console.log(err)
    })
    console.log(`Insert2 result: ${i2}`)

    let q4 = await cluster.get(unnaffectedKey2).catch((err)=>{
        console.log(`Error query4:`)
        console.log(err)
    })
    console.log(`Query4 result: ${q4}`)

    console.log("Done")



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




