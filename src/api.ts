import {Cluster} from "ioredis";


export class RedisApi {
    public cluster : Cluster;

    constructor(private host : {host: string, port: number},) {
        this.cluster = new Cluster([this.host]);
    }

}