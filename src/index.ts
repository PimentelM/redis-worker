import {Tedis} from "tedis";
// @ts-ignore
import calculateSlot from "cluster-key-slot";
import {Cluster} from "ioredis";

let run = async () => {

    let clusterNodesAddresses = [
        {host: "127.0.0.1", port: 50001},
        {host: "127.0.0.1", port: 50002},
        {host: "127.0.0.1", port: 50003},
    ];

    let cluster = new Cluster(clusterNodesAddresses);

    let nodes = clusterNodesAddresses.map(node => new Tedis(node));

    let nodeHashes = (await Promise.all(nodes.map(node => node.command("cluster", "nodes")))).map(x=>x.split(" ")[0]);


    


}

run().then(()=>console.log(`done`));







