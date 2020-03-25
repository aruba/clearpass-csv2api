#!/usr/bin/env node

const CppmApi = require('aruba-clearpass-api');

if (!global.log) {
    var log4js = require('log4js');
    global.log = log4js.getLogger();
    log.level = 'info';
}

var program = require('commander');
 
program
  .version('0.9.0')
  .helpOption('-h, --help', 'Display help for command')
  .option('-v, --verbose', 'Output extra debugging', false)
  // TODO:
  // extra fixed values
  // test headers
  // test first row
  // offset/limit
  // prefer create/prefer update
  // update-ok / create-ok
  .on('option:verbose', function() {
    if (this.verbose) log.level = 'debug';
  })
  ;
 
program
  .command('import <csv_file>')
  .description('Test connectivity, authentication and privileges')
  .option('--host <host>', 'The IP address/hostname of ClearPass.', '127.0.0.1')
  //.option('-p, --port <port>', 'Port override', 443)
  .option('--insecure', 'Disable SSL validation', false)
  .option('--client_id <client_id>', 'API Client ID', 'Client1')
  .option('--client_secret <client_secret>', 'API Client Secret', '')
  .option('--exclude <list of columns to exclude>', 'Excluded list')
  .action(importCsv);

 
program
  .command('ping')
  .description('Test connectivity, authentication and privileges')
  .option('--host <host>', 'The IP address/hostname of ClearPass.', '127.0.0.1')
  //.option('-p, --port <port>', 'Port override', 443)
  .option('--insecure', 'Disable SSL validation', false)
  .option('--client_id <client_id>', 'API Client ID', 'Client1')
  .option('--client_secret <client_secret>', 'API Client Secret', '')
  .option('--insecure', 'Disable SSL validation', false)
  .action(function(options){
    log.info('TODO - convert to /oauth/me and /oauth/privileges');
    var client = createClient(this, options);
	var o = {
		filter: {},
		sort: '-id',
		offset: 0,
		limit: 1
	};
    client.getDevices(o, function (error, data, statusCode) {
        if (error) {
            console.log(error);
        }
        else {
            if (statusCode == 200) {
                console.log('SUCCESS');
            } else {
                console.log('Unexpected code: '+statusCode, JSON.stringify(data, null, 2));
            }
        }
    });
  });

program.on('--help', () => {
  console.log('');
  console.log('Example calls:');
  console.log('  $ clearpass-csv2api --verbose import --host 172.16.247.133 --exclude=random1,random2 devices.csv');
  console.log('  $ clearpass-csv2api ping --host 172.16.247.133');
});

function createClient(program, options) {
    var client = new CppmApi({
        host: options.host,
        clientId: options.client_id,
        clientSecret: options.client_secret,
        sslValidation: !options.insecure
    });
    return client;
}

function importCsv(csv_file, options) {
    //log.debug(options);
    var client = createClient(options.parent, options);

  const csv = require('csv-parser');
  const fs = require('fs');
  const exclude = program.exclude ? program.exclude.split(',') : [];
  const results = [];
 
  fs.createReadStream(csv_file)
    .pipe(csv())
    .on('data', (data) => {
      var i = 0;
      var len = exclude.length;
      for (; i < len; i++) { 
        delete data[exclude[i]];
      }
      log.info('Creating: ', data);
      client.createDevice(data, false, function (error, result, statusCode) {
          if (error) {
              var msg = error.message;
              if (result && result.detail) {
                  msg += ' (' + result.detail + ')';
              }
              log.error(msg);
              //log.error(error);
          } else {
              if (statusCode == 201) {
                  log.debug('Created', JSON.stringify(result, null, 2));
              } else {
                  log.warn('Unknown statusCode: '+statusCode, JSON.stringify(result, null, 2));
              }
          }
      });
      //results.push(data);
    })
    .on('end', () => {
      //log.info('csv', results);
    });
}

program.parse(process.argv);
