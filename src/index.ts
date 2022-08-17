import { Command } from 'commander';
import {RedisCluster} from "./api";
import {DeleteLeaderboardWorker} from "./apps/deleteLeaderboard";
const program = new Command();

// Initialize CLI here

program.name('redis-tool').description('Mino Games - Redis Tool');


program
    .command('delete-leaderboard')
    .description('Use this command to delete a leaderboard and all keys related to it')
    .argument('eventId', 'Event Id')
    .option('--safe', 'Safe mode - sets the ttl of keys to 30 days or value specified in --ttl instead of deleting them')
    .option('--ttl <value>', 'TTL value for safe mode')
    .option('--batchSize <value>', 'Batch size to set the rate at which keys are going to be obtained and deleted', "1000")
    .option('--maxParallelism <value>', 'Maximum number of parallel calls to redis', "100")
    .requiredOption('-h, --host <value>', 'Host')
    .requiredOption('-p, --port <value>', 'Port', "6379")
    .action(async (eventId, options) => {

        let cluster = new RedisCluster({host: options.host, port: parseInt(options.port,10)});

        // options is being passed directly into the worker here, we are not validating it for now.
        let worker = new DeleteLeaderboardWorker(cluster, eventId, options);

        await worker.run();

        process.exit(0);
    });

program.
    command('health-check')
    .requiredOption('-h, --host <value>', 'Host')
    .requiredOption('-p, --port <value>', 'Port', "6379")
    .action(async (options) => {
        let cluster = new RedisCluster({host: options.host, port: parseInt(options.port,10)});

        console.log(`Querying cluster some info...`)

        let info = await cluster.clusterNodesRaw();

        console.log(info);


        process.exit(0);
    });




program.parse(process.argv);




