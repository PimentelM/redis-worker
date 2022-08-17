


// Delete leaderboard

// Scan and delete pid-username keymappings


import {RedisCluster} from "../api";
import {chunk} from "lodash";
// import pLimit from 'p-limit';

// const pLimit = require('p-limit');

interface DeleteLearderBoardWorkerOptions {
    unsafe?: boolean;
    ttl?: number;
    keyScanBatchSize: number;
    maxNumberOfParallellDeletes: number;
}

const ONE_MONTH_IN_SECONDS = 60 * 60 * 24 * 30;

const DEFAULT_OPTIONS: DeleteLearderBoardWorkerOptions = {
    keyScanBatchSize: 100,
    maxNumberOfParallellDeletes: 10,
}

export class DeleteLeaderboardWorker {
    private options : DeleteLearderBoardWorkerOptions;

    private onStopCallBacks = [];
    private onProgressCallBacks = [];

    private limitConcurrency : ( fn : () => Promise<void>) => Promise<void>;

    constructor(private redisCluster: RedisCluster,
                private eventId: string,
                options: Partial<DeleteLearderBoardWorkerOptions> = {}) {
        this.options = {...DEFAULT_OPTIONS, ...options};

        this.limitConcurrency = (fn) => fn();  //pLimit(this.options.maxNumberOfParallellDeletes);
    }

    log(msg){
        console.log(msg);
    }


    async run(){

        let leaderboardLength = await this.redisCluster.ioredis.zcard(this.getLeaderBoardZsetKey());

        this.log(`Found ${leaderboardLength} entries in leaderboard ${this.getLeaderBoardZsetKey()}`);
        if(leaderboardLength > 0){
            this.log(`Deleting leaderboard related keys...`);

            let cursor = 0;

            while(cursor <= leaderboardLength){
                this.log(`Getting keys at cursor ${cursor}...`);
                let [nextCursor, keys] = await this.scanForLeaderBoardRelatedKeys(cursor);
                this.log(`Found ${keys.length} keys to delete, starting delete...`);
                await this.deleteKeys(keys);
                cursor = nextCursor;
            }

        }

        // Delete leaderboard
        this.log(`Deleting leaderboard ${this.getLeaderBoardZsetKey()}`);
        await this.deleteLeaderBoard();

    }

    private async scanForLeaderBoardRelatedKeys(cursor: number) : Promise<[cursor: number, elements: string[]]> {
        let cursorEnd = cursor + this.options.keyScanBatchSize - 1;
        return new Promise((resolve, reject) => {
            this.redisCluster.zrange(this.getLeaderBoardZsetKey(),cursor, cursorEnd)
                .then((result)=> resolve([cursorEnd + 1, result.map(key=> this.getRelatedKeyFromPid(key))]))
                .catch(reject);
        });
    }

    private getRelatedKeyFromPid(pid){
        return `{${this.getLeaderBoardZsetKey()}}${pid}`
    }

    private async deleteLeaderBoard(){
        // Delete the Zset representing the leaderboard
        if(this.options.unsafe) {
            await this.redisCluster.del(this.getLeaderBoardZsetKey());
        } else {
            await this.redisCluster.expire(this.getLeaderBoardZsetKey(), this.options.ttl ?? ONE_MONTH_IN_SECONDS );
        }
    }

    private async deleteKeys(keys: string[]) : Promise<void> {
        // Delete a simple key
        if(this.options.unsafe) {
            await this.redisCluster.del(...keys);
        } else {
            for (let keysBatch of chunk(keys, this.options.maxNumberOfParallellDeletes)){
                await Promise.all(keysBatch.map(key=> this.redisCluster.expire(key, this.options.ttl ?? ONE_MONTH_IN_SECONDS)));
            }
        }

    }

    private getLeaderBoardZsetKey(){
        return this.eventId;
    }


}

