import {RedisCluster} from "./api";

interface KeyTargets {
    zset?: string;
    hash?: string;
}

class Report {
    constructor(
        public startTime: Date,
        public stopTime: Date,
        public keyMisses: number[],
        public zsetMisses: number[],
        public hashMisses: number[],
        public nOfCycles: number,
        public cycleInterval: number,
    ) { }

    get percentageOfZSetMisses(): number {
        return this.zsetMisses.length / this.nOfCycles;
    }

    get percentageOfHashMisses(): number {
        return this.hashMisses.length / this.nOfCycles;
    }

    get percentageOfKeyMisses(): number {
        return this.keyMisses.length / this.nOfCycles;
    }

    get percentageOfMisses(): number {
            return (this.percentageOfHashMisses + this.percentageOfZSetMisses + this.percentageOfKeyMisses) / 3
    }

    public timestampOfCycle(cycle: number): number {
        return this.startTime.getTime() + (cycle * this.cycleInterval);
    }


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

    private startTime?: Date;
    private stopTime?: Date;

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
        this.startTime = new Date();
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

        this.stopTime = new Date();
        this.status = 'stopped';
        this.stopCallBacks.forEach(cb => cb(report));
    }

    private async executeCycle(){
        let key = this.keyPrefix + ':cycle:' + this.cycleCount;
        let currentCycle = this.cycleCount;

        // Write to zset, set timeout to check existence of element in zset
        this.cluster.zadd(this.targets.zset!, currentCycle, key).then(()=>{
            setTimeout(async ()=>{
                let result = await this.cluster.zscore(this.targets.zset!,key).catch(()=>-1)
                if(result != currentCycle){
                    this.zsetMiss.push(currentCycle);
                }
            }, this.readAfterWriteDelay);
        }).catch(err=>{
            this.zsetMiss.push(currentCycle);
        })

        // Write to hash, set timeout to check existence of element in hash
        this.cluster.hset(this.targets.hash!, key, currentCycle).then(()=>{
            setTimeout(async ()=>{
                let result = await this.cluster.hget(this.targets.zset!,key).catch(()=>-1)
                if(result != currentCycle){
                    this.hashMiss.push(currentCycle);
                }
            }, this.readAfterWriteDelay);
        }).catch(err=>{
            this.hashMiss.push(currentCycle);
        })


        // Create new Key, set timeout to check existence of key
        this.cluster.set(key, currentCycle).then(()=>{
            setTimeout(async ()=>{
                let result = await this.cluster.get(key).catch(()=>-1)
                if(result != currentCycle){
                    this.keyMiss.push(currentCycle);
                }
            }, this.readAfterWriteDelay);
        }).catch(err=>{
            this.keyMiss.push(currentCycle);
        })

        this.cycleCount++;
    }

    public makeReport() : Report {
        return new Report(
            this.startTime!,
            this.stopTime!,
            this.keyMiss,
            this.zsetMiss,
            this.hashMiss,
            this.cycleCount,
            this.intervalBetweenCycles,
        )
    }


    public onStop(callback: (report: Report) => void){
        this.stopCallBacks.push(callback);
    }



}