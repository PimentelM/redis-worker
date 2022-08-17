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
    .requiredOption('-h, --host <value>', 'Host')
    .requiredOption('-p, --port <value>', 'Port')
    .action(async (eventId, options) => {

        let cluster = new RedisCluster({host: options.host, port: options.port});

        // options is being passed directly into the worker here, we are not validating it for now.
        let worker = new DeleteLeaderboardWorker(cluster, eventId, options);

        await worker.run();

        process.exit(0);
    });

program.parse(process.argv);




