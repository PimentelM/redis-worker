import {RedisCluster} from "./api";

interface KeyTargets {
    zset?: string;
    hash?: string;
}

class Events{
    public entries : string[] = [];

    public push(event: string){
        event = `${new Date().toISOString()}: ${event}`
        this.entries.push(event);
        console.log(event)
    }
}

export class DownTimeCheckerWorker {
    private status: string = 'ready'
    private stopCallBacks: Function[] = [];

    private cycleIntervalIdentifier: any;
    private durationTimeoutIdentifier: any;

    private zsetMiss: number[] = [];
    private keyMiss: number[] = [];
    private hashMiss: number[] = [];

    private zsetMissingAfter: number[] = [];
    private keyMissingAfter: number[] = [];
    private hashMissingAfter: number[] = [];

    private cycleCount = 0;

    private startTime?: Date;
    private stopTime?: Date;

    events: Events = new Events();
    private allPromises: Promise<any>[] = [];

    constructor(
        private cluster: RedisCluster,
        private targets: KeyTargets,
        private keyPrefix: string,
        private duration: number,
        private intervalBetweenCycles: number,
        private readAfterWriteDelay: number,
    ) {
        if (!this.targets.zset) {
            this.targets.zset = this.keyPrefix + ':zset';
        }

        if (!this.targets.hash) {
            this.targets.hash = this.keyPrefix + ':hash';
        }
    }

    public start() {
        if (this.status === 'running') {
            throw new Error('Worker is already running')
        }
        if (['done', 'stopped'].includes(this.status)) {
            throw new Error('Worker already finished execution')
        }
        if (this.status !== 'ready') {
            throw new Error('Worker is in an invalid state')
        }
        this.startTime = new Date();
        this.cycleIntervalIdentifier = setInterval(async () => {
            await this.executeCycle()
        }, this.intervalBetweenCycles);

        this.durationTimeoutIdentifier = setTimeout(async () => {
            this.events.push(`[${this.cycleCount}] DURATION TIMEOUT`);
            await this.stop();
        }, this.duration);

        this.events.push(`STARTED`);
        this.status = 'running';
    }

    public async stop() {
        if (this.status !== 'running') {
            throw new Error('Worker is not running')
        }
        // Stop Cycles
        clearInterval(this.cycleIntervalIdentifier);

        // Stop duration timeout
        clearTimeout(this.durationTimeoutIdentifier);

        this.stopTime = new Date();
        this.status = 'stopped';
        this.events.push(`STOPPED`);

        await Promise.all(this.allPromises);
        this.cycleCount--;

        // Check that every data that was written is still in place
        await this.checkAllWrites();


        this.events.push(`DONE`);
        this.status = 'done';
        this.stopCallBacks.forEach(cb => cb(this));
    }

    private checkZset(currentCycle: number){
        let key = this.keyPrefix + ':cycle:' + currentCycle;
        return this.cluster.zadd(this.targets.zset!, currentCycle, key).then(() => {
            setTimeout(async () => {
                let result = await this.cluster.zscore(this.targets.zset!, key).catch((err) => {
                    this.events.push(`[${currentCycle}][ZSET] Failed to read ${key} from zset ${this.targets.zset} \nERR: ${err.message}`)
                    return `ERROR`
                })
                if (result != currentCycle) {
                    this.events.push(`Query to ${key} of zset ${this.targets.zset} returned ${result} instead of ${currentCycle}`);
                    this.zsetMiss.push(currentCycle);
                }
            }, this.readAfterWriteDelay);
        }).catch(err => {
            this.events.push(`Failed to add element ${currentCycle} to zset ${this.targets.zset}\n ERR:${err.message}`);
            this.zsetMiss.push(currentCycle);
        })
    }

    private checkHash(currentCycle: number){
        let key = this.keyPrefix + ':cycle:' + currentCycle;
        return this.cluster.hset(this.targets.hash!, key, currentCycle).then(() => {
            setTimeout(async () => {
                let result = await this.cluster.hget(this.targets.hash!, key).catch(err => {
                    this.events.push(`[${currentCycle}][HASH] Failed to read ${key} from hash ${this.targets.hash} \nERR: ${err.message}`)
                    return `ERROR`
                })
                if (result != currentCycle) {
                    this.events.push(`Query to ${key} of hash ${this.targets.hash} returned ${result} instead of ${currentCycle}`);
                    this.hashMiss.push(currentCycle);
                }
            }, this.readAfterWriteDelay);
        }).catch(err => {
            this.events.push(`Failed to set field ${key} with value ${currentCycle} at hash ${this.targets.hash}\n ERR:${err.message}`);
            this.hashMiss.push(currentCycle);
        })
    }

    private checkSimpleKey(currentCycle: number){
        let key = this.keyPrefix + ':cycle:' + currentCycle;
        return this.cluster.set(key, currentCycle).then(() => {
            setTimeout(async () => {
                let result = await this.cluster.get(key).catch(err => {
                    this.events.push(`[${currentCycle}][KEY] Failed to read ${key} \nERR: ${err.message}`)
                    return `ERROR`
                })
                if (result != currentCycle) {
                    this.events.push(`Query to ${key} returned ${result} instead of ${currentCycle}`);
                    this.keyMiss.push(currentCycle);
                }
            }, this.readAfterWriteDelay);
        }).catch(err => {
            this.events.push(`Failed to set key ${key} with value ${currentCycle}\n ERR:${err.message}`);
            this.keyMiss.push(currentCycle);
        })
    }

    private async executeCycle() {
        let currentCycle = this.cycleCount;

        this.events.push(`[${currentCycle}] CYCLE STARTED`);

        // Write to zset, set timeout to check existence of element in zset
        let p1 = this.checkZset(currentCycle);

        // Write to hash, set timeout to check existence of element in hash
        let p2 = this.checkHash(currentCycle);

        // Create new Key, set timeout to check existence of key
        let p3 = this.checkSimpleKey(currentCycle);

        let cyclePromise = Promise.all([p1, p2, p3]).then(() => {
            this.events.push(`[${currentCycle}] CYCLE FINISHED`);
        })

        this.allPromises.push(cyclePromise);

        this.cycleCount++;
    }


    public onStop(callback: (instance: DownTimeCheckerWorker) => void) {
        this.stopCallBacks.push(callback);
    }


    private async checkAllWrites() {
        for (let cycle = 0; cycle <= this.cycleCount; cycle++) {
            this.events.push(`[${cycle} - ${this.cycleCount}] CHECKING CYCLE INPUTS`);
            let exists;
            // Check that key exists
            let key = this.keyPrefix + ':cycle:' + cycle;
            exists = await this.cluster.get(key).catch(err => null).then(result => Number(result) === cycle)
            if (!exists) {
                this.events.push(`[${cycle}] KEY ${key} MISSING`);
                this.keyMissingAfter.push(cycle);
            }

            // Check that element exists in zset
            exists = await this.cluster.zscore(this.targets.zset!, key).catch(err => null).then(result => Number(result) === cycle)
            if(!exists){
                this.events.push(`[${cycle}] ZSET MISSING AT ${this.targets.zset}`);
                this.zsetMissingAfter.push(cycle);
            }

            // Check that element exists in hash
            exists = await this.cluster.hget(this.targets.hash!, key).catch(err => null).then(result => Number(result) === cycle)
            if(!exists){
                this.events.push(`[${cycle}] HASH MISSING AT ${this.targets.hash}`);
                this.hashMissingAfter.push(cycle);
            }
        }
    }

    // Report methods
    get percentageOfZSetMisses(): number {
        return this.zsetMiss.length / this.cycleCount;
    }

    get percentageOfHashMisses(): number {
        return this.hashMiss.length / this.cycleCount;
    }

    get percentageOfKeyMisses(): number {
        return this.keyMiss.length / this.cycleCount;
    }

    get downtime(): number {
        return (this.percentageOfHashMisses + this.percentageOfZSetMisses + this.percentageOfKeyMisses) / 3
    }

    get percentageOfKeyMissingAfter(): number {
        return this.keyMissingAfter.length / this.cycleCount;
    }

    get percentageOfZSetMissingAfter(): number {
        return this.zsetMissingAfter.length / this.cycleCount;
    }

    get percentageOfHashMissingAfter(): number {
        return this.hashMissingAfter.length / this.cycleCount;
    }

    get dataLoss(): number {
        return (this.percentageOfHashMissingAfter + this.percentageOfZSetMissingAfter + this.percentageOfKeyMissingAfter) / 3
    }

    public timestampOfCycle(cycle: number): number {
        return this.startTime!.getTime() + (cycle * this.intervalBetweenCycles);
    }


}