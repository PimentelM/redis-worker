import { Command } from 'commander';
const program = new Command();

// Initialize CLI here


program.name('redis-tool').description('Mino Games - Redis Tool');


program
    .command('delete-leaderboard')
    .description('Use this command to delete a leaderboard and all keys related to it')
    .argument('eventId', 'Event Id')
    .option('--unsafe <value>', 'Unsafe mode - delete keys straight away, instead of setting the ttl of keys to 30 days or value specified in --ttl', false)
    .option('--ttl <value>', 'TTL value for safe mode')
    .action(async (eventId, options) => {
        // Call async method to delete leaderboard here...

    });





