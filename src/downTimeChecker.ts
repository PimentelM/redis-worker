import {RedisCluster} from "./api";

interface KeyTargets {
    zset?: string;
    hash?: string;
}

interface Report {

}

class DownTimeCheckerWorker {
    private status: string = 'ready'
    private stopCallBacks : Function[] = [];

    private cycleIntervalIdentifier : any;
    private durationTimeoutIdentifier: any;

    private zsetMiss : number[] = [];
    private keyMiss : number[]= [];
    private hashMiss : number[]= [];

    private cycleCount = 0;

    constructor(
        private cluster: RedisCluster,
        private targets: KeyTargets,
        private keyPrefix: string,
        private duration: number,
        private intervalBetweenCycles : number,
        private readAfterWriteDelay: number,
    ) {
        if(!this.targets.zset){
            this.targets.zset = this.keyPrefix + ':zset';
        }

        if(!this.targets.hash){
            this.targets.hash = this.keyPrefix + ':hash';
        }
    }

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


        this.cycleIntervalIdentifier = setInterval(async()=>{
            await this.executeCycle()
        }, this.intervalBetweenCycles);

        this.durationTimeoutIdentifier = setTimeout(()=>{
            this.stop();
        }, this.duration);


        this.status = 'running';
    }

    public stop(){
        if(this.status !== 'running'){
            throw new Error('Worker is not running')
        }

        // Stop Cycles
        clearInterval(this.cycleIntervalIdentifier);

        // Stop duration timeout
        clearTimeout(this.durationTimeoutIdentifier);

        // Create report
        let report = this.makeReport();


        this.status = 'stopped';
        this.stopCallBacks.forEach(cb => cb(report));
    }

    private async executeCycle(){
        let key = this.keyPrefix + ':cycle:' + this.cycleCount;
        let currentCycle = this.cycleCount;

        // Write to zset, set timeout to check existence of element in zset
        this.cluster.zadd(this.targets.zset!, currentCycle, key).then(()=>{
            setTimeout(async ()=>{
                let result = await this.cluster.zscore(this.targets.zset!,key)
                if(result != currentCycle){
                    this.zsetMiss.push(currentCycle);
                }
            }, this.readAfterWriteDelay);
        })

        // Write to hash, set timeout to check existence of element in hash
        this.cluster.hset(this.targets.hash!, key, currentCycle).then(()=>{
            setTimeout(async ()=>{
                let result = await this.cluster.hget(this.targets.zset!,key)
                if(result != currentCycle){
                    this.hashMiss.push(currentCycle);
                }
            }, this.readAfterWriteDelay);
        })


        // Create new Key, set timeout to check existence of key
        this.cluster.set(key, currentCycle).then(()=>{
            setTimeout(async ()=>{
                let result = await this.cluster.get(key)
                if(result != currentCycle){
                    this.keyMiss.push(currentCycle);
                }
            }, this.readAfterWriteDelay);
        })

        this.cycleCount++;
    }

    public makeReport() : Report {

        return {};
    }


    public onStop(callback: Function){
        this.stopCallBacks.push(callback);
    }



}