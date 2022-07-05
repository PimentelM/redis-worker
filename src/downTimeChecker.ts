import {RedisCluster} from "./api";

interface KeyTargets {
    zset?: string;
    hash?: string;
}

class DownTimeCheckerWorker {
    private status: string = 'ready'
    private stopCallBacks : Function[] = [];


    private zsetMiss = [];
    private keyMiss = [];
    private hashMiss = [];

    private count = 0;

    constructor(
        private cluster: RedisCluster,
        private targets: KeyTargets,
        private keyPrefix: string,
        private duration: number,
        private intervalBetweenWrites : number,
        private readAfterWriteDelay: number,
    ) {}

    public start(){
        if(this.status === 'running'){
            throw new Error('Worker is already running')
        }
        if(['done', 'stopped'].includes(this.status)){
            throw new Error('Worker already finished execution')
        }
        if(this.status !== 'ready'){
            throw new Error('Worker is in an invalid state')
        }

        // Core executionner
        
        // Write to zset, set timeout to check existence of element in zset

        // Write to hash, set timeout to check existence of element in hash

        // Create new Key, set timeout to check existence of key





        this.status = 'running';
    }

    public stop(){
        if(this.status !== 'running'){
            throw new Error('Worker is not running')
        }

        // Create report



        this.status = 'stopped';
        this.stopCallBacks.forEach(cb => cb());
    }

    public onStop(callback: Function){
        this.stopCallBacks.push(callback);
    }



}