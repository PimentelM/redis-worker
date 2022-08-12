


// Delete leaderboard

// Scan and delete pid-username keymappings


import {RedisCluster} from "../api";
import {chunk} from "lodash";
import pLimit from 'p-limit';

interface DeleteLearderBoardWorkerOptions {
    unsafe?: boolean;
    ttl?: number;
    keyScanBatchSize: number;
    keyDeleteBatchSize: number;
    maxNumberOfParallellDeletes: number;
}

const ONE_MONTH_IN_SECONDS = 60 * 60 * 24 * 30;

const DEFAULT_OPTIONS: DeleteLearderBoardWorkerOptions = {
    keyScanBatchSize: 1000,
    keyDeleteBatchSize: 250,
    maxNumberOfParallellDeletes: 10,
}

class DeleteLeaderBoardWorker {
    private options : DeleteLearderBoardWorkerOptions;

    private onStopCallBacks = [];
    private onProgressCallBacks = [];

    private limitConcurrency : ( fn : () => Promise<void>) => Promise<void>;

    constructor(private redisCluster: RedisCluster,
                private eventId: string,
                options: Partial<DeleteLearderBoardWorkerOptions> = {}) {
        this.options = {...DEFAULT_OPTIONS, ...options};

        this.limitConcurrency = pLimit(this.options.maxNumberOfParallellDeletes);
    }


    async run(){

        // Delete leaderboard
        await this.deleteLeaderBoard();

        // Start scanning for leaderboard related keys and deleting them in batches
        let cursor = "0";
        do {
            let [newCursor, elements] = await this.scanForLeaderBoardRelatedKeys(cursor);

            // Delete all the keys in batches while limiting concurrency
            for(let batch of chunk(elements, this.options.keyDeleteBatchSize)){
                await Promise.all(batch.map(key =>
                    this.limitConcurrency( () => this.deleteKey(key))));
            }

            cursor = newCursor;
        } while(cursor !== "0");
    }

    private async scanForLeaderBoardRelatedKeys(cursor: string) : Promise<[cursor: string, elements: string[]]> {
        return new Promise((resolve, reject) => {
            this.redisCluster.ioredis.scan(cursor,
                "MATCH", this.getPidUserNameKeyMatchPattern(),
            "COUNT", this.options.keyScanBatchSize,
                "TYPE", "string")
                .then(resolve)
                .catch(reject);
        });
    }

    private getPidUserNameKeyMatchPattern(){
        return `{${this.getLeaderBoardZsetKey()}}*`
    }

    private async deleteLeaderBoard(){
        // Delete the Zset representing the leaderboard
        if(this.options.unsafe) {
            await this.redisCluster.del(this.getLeaderBoardZsetKey());
        } else {
            await this.redisCluster.expire(this.getLeaderBoardZsetKey(), this.options.ttl ?? ONE_MONTH_IN_SECONDS );
        }
    }

    private async deleteKey(key: string) : Promise<void> {
        // Delete a simple key
        if(this.options.unsafe) {
            await this.redisCluster.del(key);
        } else {
            await this.redisCluster.expire(key, this.options.ttl ?? ONE_MONTH_IN_SECONDS );
        }

    }

    private getLeaderBoardZsetKey(){
        return this.eventId;
    }






}

