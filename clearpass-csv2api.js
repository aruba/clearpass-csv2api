#!/usr/bin/env node

const CppmApi = require('aruba-clearpass-api');
const async = require('async');

if (!global.log) {
    const log4js = require('log4js');
    global.log = log4js.getLogger();
    log.level = 'info';
}

const IMPORTING =
{
    "device": {
        "key": "mac",
        "aliases": ["device", "devices"],
        "privilege_create": "mac_create",
        "privilege_edit": "full-user-control",
        "required": ["role_id"],
        "CppmApiCreate": "createDevice",
        "CppmApiReplace": "replaceDeviceByMac",
        "CppmApiUpdate": "updateDeviceByMac"
    },
    "guest": {
        "key": "username",
        "aliases": ["guest", "guests", "guestuser", "guestusers"],
        "privilege_create": "create_user",
        "privilege_edit": "full-user-control",
        "required": ["password", "role_id"],
        "CppmApiCreate": "createGuest",
        "CppmApiReplace": "replaceGuestByUserName",
        "CppmApiUpdate": "updateGuestByUserName"
    },
    /*"endpoint": {
        "key": "mac",
        "aliases": ["endpoint", "endpoints"],
        "privilege_create": "cppm_endpoints",
        "privilege_edit": "cppm_endpoints",
        "required": [],
        "CppmApiCreate": "createEndpoint",
        "CppmApiReplace": "replaceEndpointByMac",
        "CppmApiUpdate": "updateEndpointByMac"
    }*/ 
};

const program = require('commander');
 
program
    .version('0.9.5')
    .helpOption('-h, --help', 'Display help for command')
    .option('-v, --verbose', 'Output extra debugging', false)
    .option('--importing <importing>', 'What is being imported.  Current accepted values: ' + Object.keys(IMPORTING).join(', '), 'device') // Match names from API Explorer
    // TODO:
    // prefer create/prefer update
    // update-ok / create-ok
    .on('option:verbose', function() {
        if (this.verbose) log.level = 'debug';
    })
    .on('option:importing', function() {
        this.importing = this.importing.toLowerCase();
        let importingMatch = false;
        for (let import_key in IMPORTING) {
            if (IMPORTING.hasOwnProperty(import_key)) {
                if (import_key == this.importing) {
                    importingMatch = true;
                    break;
                }
                let imp = IMPORTING[import_key];
                for (i=0; i < imp.aliases.length; i++) {
                    if (imp.aliases[i] == this.importing) {
                        importingMatch = true;
                        this.importing = import_key;
                        break;
                    }
                }
            }
            if (importingMatch) {
                break;
            }
        }
        if (!importingMatch) {
            console.error('Invalid value for: --importing (Allowed: ' + Object.keys(IMPORTING).join(', ') + ')');
            process.exit(1);
        }
    })
    ;

// Main import function
program
    .command('import <csv_file>'/*, { isDefault: true }*/)
    .description('Import a CSV of items into ClearPass')
    .option('--host <host>', 'The IP address/hostname of ClearPass.', '127.0.0.1')
    //.option('-p, --port <port>', 'Port override', 443)
    .option('--insecure', 'Disable SSL validation', false)
    .option('--client_id <client_id>', 'API Client ID', 'Client1')
    .option('--client_secret <client_secret>', 'API Client Secret', '')
    .option('-x --extra <extra>', 'An extra key=value pair. Multiple supported.', collectPairs, {})
    .option('--exclude <list of columns to exclude>', 'Excluded list', [])
    .option('--sessions <sessions>', 'Max simultaneous sessions', 1)
    .option('--strategy <strategy>', `Strategy to use when making API calls
    create-only       Create new items only.  Existing will be ignored.
    create-or-replace Create new items or replace existing entries with the CSV.
    create-or-update  Create new items or update existing entries with data in the CSV.
    update-only       Update existing items only.  Entries without a match will be skipped.
    update-or-create  Update existing items.  If the entry does not exist it will be created.
    replace-only      Replace existing items only.  Entries without a match will be skipped.
    replace-or-create Replace existing items.  If the entry does not exist it will be created.
    `, 'create-only')
    .option('--change-of-authorization', 'Send RADIUS CoA requests', false) // changeOfAuthorization
    .option('--dry-run', 'Dry run, do not send any API', false) // dryRun
    .on('option:exclude', function() {
        this.exclude = typeof(this.exclude) == 'string' ? this.exclude.split(',') : [];
    })
    .on('option:strategy', function() {
        let strategies = [
            'create-only', // POST
            'create-or-replace', // POST-PUT
            'create-or-update', // POST-PATCH
            'update-only', // PATCH
            'update-or-create', // PATCH-POST
            'replace-only', // PUT
            'replace-or-create', // PUT-POST
        ];
        if (!strategies.includes(this.strategy)) {
            console.error('Invalid value for: --strategy (Allowed: ' + strategies.join(', ') + ')');
            process.exit(1);
        }
    })
    .action(commandActionImportCsv);

// Takes the first two lines of the csv and outputs what would have been created.
program
    .command('testcsv <csv_file>'/*, { isDefault: true }*/)
    .description('Confirm the csv seems valid.')
    .option('-x --extra <extra>', 'An extra key=value pair. Multiple supported.', collectPairs, {})
    .option('--exclude <list of columns to exclude>', 'Excluded list', [])
    .on('option:exclude', function() {
        this.exclude = typeof(this.exclude) == 'string' ? this.exclude.split(',') : [];
    })
    .action(commandActionTestCSV);

// Confirm connectivity settings and permissions
program
    .command('ping')
    .description('Test connectivity, authentication and privileges')
    .option('--host <host>', 'The IP address/hostname of ClearPass.', '127.0.0.1')
    //.option('-p, --port <port>', 'Port override', 443)
    .option('--insecure', 'Disable SSL validation', false)
    .option('--client_id <client_id>', 'API Client ID', 'Client1')
    .option('--client_secret <client_secret>', 'API Client Secret', '')
    .option('--insecure', 'Disable SSL validation', false)
    .on('--help', () => {
        console.log('');
        console.log('API Clients are created in Guest » Administration » API Services » API Clients.');
        console.log('You need the following set:');
        console.log('  * Operating Mode: ClearPass REST API');
        console.log('  * Operator Profile: A profile with sufficient privileges');
        console.log('  * Grant Type: Client Credentials');
        console.log('The Client ID and Client Secret are passed as arguments.  Ensure you protect the secret.');
        console.log('');
        console.log('Privileges:');
        console.log('  * ALL  : API Services > Allow API Access');
        console.log('  * Devices  : Devices > Create New Device and Guest Manager > Full User Control');
        console.log('  * Guests   : Guest Manager > Create New Guest Account and Guest Manager > Full User Control');
        //console.log('  * Endpoints: Policy Manager > Identity - Endpoints');
        console.log('');})
    .action(commandActionPing);

// Generate a randomized csv
program
    .command('generate <count>')
    .description('Generate a random CSV')
    .option('-x --extra <extra>', 'An extra key=value pair. Multiple supported.', collectPairs, {})
    .action(commandActionGenerate);

program.on('--help', () => {
    console.log('');
    console.log('Example calls:');
    console.log('');
    console.log('  Confirm credentials and permissions:');
    console.log('  $ clearpass-csv2api ping --host=192.0.2.10 --client_id=Client1 --client_secret=asdfadsf');
    console.log('');
    console.log('  Test the CSV:');
    console.log('  $ clearpass-csv2api testcsv devices.csv');
    console.log('  $ clearpass-csv2api --importing=guest testcsv guest.csv');
    console.log('');
    console.log('  Initiate the import:');
    console.log('  $ clearpass-csv2api --verbose import --host 192.0.2.10 --client_id=Client1 --client_secret=asdfadsf devices.csv');
    console.log('  $ clearpass-csv2api --verbose import --host 192.0.2.10 --client_id=Client1 --client_secret=asdfadsf --strategy=update-or-create devices-sync.csv');
    console.log('  $ clearpass-csv2api --verbose --importing=guest import --host 192.0.2.10 --client_id=Client1 --client_secret=asdfadsf guests.csv');
    console.log('  $ clearpass-csv2api --verbose import --host 192.0.2.10 --client_id=Client1 --client_secret=asdfadsf --exclude=id,region -x "notes=Import from ACME" -x "visitor_company=IoT Corp" devices.csv');
    console.log('');
    console.log('  Generate test data:');
    console.log('  $ clearpass-csv2api generate 100 > devices-100.csv');
    console.log('  $ clearpass-csv2api generate 100 -x role_id=1 -x "notes=Import from ACME" > devices-ACME-Contractors.csv');
    console.log('  $ clearpass-csv2api --importing=guest generate 100 > guests-100.csv');
});

/**
 * Need to accumulate -x --extra
 */
function collect(value, previous) {
    return previous.concat([value]);
}

/**
 * Need to accumulate -x --extra
 */
function collectPairs(value, previous) {
    let eq = value.indexOf('=');
    if (eq <= 0 || eq == value.length) {
        console.error("Expected 'key=value' pair: ", value);
        process.exit(1);
    }
    let key = value.substring(0, eq);
    let val = value.substring(eq +1);
    if (/\W/.test(key)) {
        console.error("Key can only contain letters, numbers and underscore 'key=value' pair: ", key);
        process.exit(1);
    }
    previous[key] = val;
    return previous;
}

function createClient(options) {
    let client = new CppmApi({
        host: options.host,
        clientId: options.client_id,
        clientSecret: options.client_secret,
        sslValidation: !options.insecure
    });
    return client;
}

function normalizeData(data, options) {
    let i = 0;
    let len = options.exclude.length;
    for (; i < len; i++) { 
        delete data[options.exclude[i]];
    }
    Object.assign(data, options.extra);
    return data;
}

function validateHeaders(headers, options, imp) {
    let missing = [];
    if (!headers.includes(imp.key)) {
        missing.push(imp.key);
    }
    for (let i = 0; i < imp.required.length; i++) {
        if (!headers.includes(imp.required[i])) {
            missing.push(imp.required[i]);
        }
    }
    if (missing.length > 0) {
        return 'The following headers are required: ' + missing.join(', ');
    }

    return true;
}

function validateData(data, options, imp) {
    // Check required fields.  Skips when unset as that must be checked above.
    let missing = [];
    let key = '?';
    if (typeof(data[imp.key]) != 'undefined' && data[imp.key] == '') {
        missing.push(imp.key);
    } else {
        key = data[imp.key];
    }
    for (let i = 0; i < imp.required.length; i++) {
        if (typeof(data[imp.required[i]]) != 'undefined' && data[imp.required[i]] == '') {
            missing.push(imp.required[i]);
        }
    }
    if (missing.length > 0) {
        return key + ' - SKIPPING: ' + 'The following fields are empty: ' + missing.join(', ');
    }

    // NULL checks
    // Note if a row is missing a column, csv-parser outright skips it.  You will not get null.
    // That is indicative of worse problems though.

    return true;
}

function handleAPIClientError(stats, key, error, verbose) {
    let response = error.response;
    if (!response) {
        console.error(key + ' - ERROR: ' + 'Unknown error:', JSON.stringify(error, null, 2));
        stats.errors++;
    } else if (response.status == 400 && response.data && response.data.title == 'invalid_client') {
        stats.errors++;
        console.error(key + ' - ERROR: ' + 'Bad keys?:', response.data.detail);
    } else if (response.status == 404 && response.data && response.data.title == 'Not Found') {
        // Does not exists
        stats.not_exists++;
        console.error(key + ' - ERROR: ' + (response.data.detail || 'Not found'));
    } else if (response.status == 422 && response.data && response.data.result && response.data.result.user_exists) {
        // Exists
        stats.exists++;
        console.error(key + ' - ERROR: ' + (response.data.result.message || 'Already exists'));
    } else if (response.status == 422 && response.data) {
        stats.errors++;
        console.error(key + ' - ERROR: ' + response.data.detail, response.data.validation_messages);
    } else {
        stats.errors++;
        console.error(key + ' - ERROR: ' + 'Unknown status: ' + response.status, JSON.stringify(response.data, null, 2));
    }
}

async function commandActionPing(options) {
    let verbose = options.parent.verbose;
    let client = createClient(options);
    client.getMyInfo(function (error, result, statusCode) {
        if (error) {
            let msg = error.message;
            if (result && result.detail) {
                msg += ' (' + result.detail + ')';
            }
            console.error('FAILED: ' + msg);
        }
        else {
            if (statusCode == 200) {
                console.log('SUCCESS', result);

                client.getMyPrivileges(function (error, result, statusCode) {
                    if (error) {
                          let msg = error.message;
                          if (result && result.detail) {
                              msg += ' (' + result.detail + ')';
                          }
                        console.error('FAILED: ' + msg);
                    }
                    else {
                        if (statusCode == 200) {
                            let imp = IMPORTING[options.parent.importing];
                            if (!Array.isArray(result.privileges)) {
                                console.error('FAILED: ' + "Missing 'privileges'");
                            } else {
                                if (result.privileges.includes(imp.privilege_create)) {
                                    console.log('SUCCESS', 'Create privilege confirmed');
                                } else {
                                    console.error('FAILED: ' + "Missing '" + imp.privilege_create + "' privilege");
                                }
                                if (result.privileges.includes(imp.privilege_edit)) {
                                    console.log('SUCCESS', 'Edit privilege confirmed');
                                } else {
                                    console.error('FAILED: ' + "Missing '" + imp.privilege_edit + "' privilege");
                                }
                            }
                        } else {
                            console.warn('Unexpected code: ' + statusCode, JSON.stringify(result, null, 2));
                        }
                    }
                });
            } else {
                console.warn('Unexpected code: ' + statusCode, JSON.stringify(result, null, 2));
            }
        }
    });
}

/**
 * Generate random CSV.  Similar to other scripts, all logging is stderr and stdout will be the CSV itself.
 */
function commandActionGenerate(count, options) {
    let verbose = options.parent.verbose;
    let imp = IMPORTING[options.parent.importing];
    count = parseInt(count);
    if (isNaN(count) || count <= 0) {
        console.error('Count must be an integer');
        process.exit(1);
    }

    let extra_keys = '';
    let extra_values = '';
    for (let extra_key in options.extra) {
        if (options.extra.hasOwnProperty(extra_key)) {
            extra_keys += ',' + extra_key;
            let extra_value = options.extra[extra_key];
            if (extra_value.replace(/ /g, '').match(/[\s,"]/)) {
                extra_value =  '"' + extra_value.replace(/"/g, '""') + '"';
            }
            extra_values += ',' + extra_value;
        }
    }

    if (options.parent.importing == 'device') {
        let ceil = 16777215;
        if (count > ceil) {
            console.error('Count must be less than %d', ceil);
            process.exit(1);
        }
        let prefix = Math.floor(Math.random() * ceil);
        let prefix_str = prefix.toString(16).toUpperCase();
        prefix *= 256*256*256;
        let header = 'mac';
        let has_role_id = options.extra['role_id'];
        if (!has_role_id) {
            header += ',role_id';
        }
        let has_visitor_name = options.extra['visitor_name'];
        if (!has_visitor_name) {
            header += ',visitor_name';
        }
        console.log(header + extra_keys);
        for (let i = 0; i < count; i++) {
            let mac = prefix + i;
            mac = mac.toString(16).toUpperCase();
            while (mac.length < 12) {
                mac = "0" + mac;
            }
            mac = mac.match(/.{2}/g).join('-');
            let row = mac;
            if (!has_role_id) {
                row += ',3';
            }
            if (!has_visitor_name) {
                row += ',Device ' + prefix_str + ' and ' + i;
            }
            console.log(row + extra_values);
        }
    } else if (options.parent.importing == 'guest') {
        let ceil = 16777215; // Arbitrary for Guests
        if (count > ceil) {
            console.error('Count must be less than %d', ceil);
            process.exit(1);
        }
        let prefix = Math.floor(Math.random() * ceil);
        let prefix_str = prefix.toString(16).toUpperCase();
        
        console.log('username,password,role_id,visitor_name' + extra_keys);
        for (let i = 0; i < count; i++) {
            let username = prefix +'_' + String(i).padStart(6, '0');
            let password = username.split('').reverse().join('');
            console.log('%s,%s,%d,"%s"%s', username, password, 3, 'Device ' + prefix_str + ' and '+i, extra_values);
        }
    } else {
        console.error("Importing type not yet supported in the generator: ", options.parent.importing);
    }
}

function commandActionTestCSV(csv_file, options) {

    const csv = require('csv-parser');
    const fs = require('fs');
    const errors = {};
    let imp = IMPORTING[options.parent.importing];
    let verbose = options.parent.verbose;
    let first = true;
   
    fs.createReadStream(csv_file)
        .pipe(csv({ strict: true }))
        .on('headers', (headers) => {
            // Catches EFBBBF (UTF-8 BOM) because the buffer-to-string
            // conversion translates it to FEFF (UTF-16 BOM)
            if (headers[0].charCodeAt(0) === 0xFEFF) {
                headers[0] = headers[0].slice(1);
                this.headers = headers;
            }
            let validated = validateHeaders(headers, options, imp);
            if (validated !== true) {
                if (errors[validated]) {
                    errors[validated]++;
                } else {
                    errors[validated] = 1;
                }
            }
        })
        .on('data', (data) => {
            //console.log(data);
            data = normalizeData(data, options);
            if (first) {
                first = false;
                console.log('Review this structure for correctness:');
                console.log(JSON.stringify(data, null, 2));
            }
            let validated = validateData(data, options, imp);
            if (validated !== true) {
                if (errors[validated]) {
                    errors[validated]++;
                } else {
                    errors[validated] = 1;
                }
            }
        })
        .on('error', (error) => {
            if (errors[error.message]) {
                errors[error.message]++;
            } else {
                errors[error.message] = 1;
            }
            console.log('error', error.message); // TODO: end is only called when no on.error is called!!!
        })
        .on('end', () => { // TODO: end is only called when no on.error is called!!!
            let error_messages = Object.keys(errors);
            if (error_messages.length > 0) {
                console.error('The following errors were reported:');
                if (verbose) {
                    console.error(JSON.stringify(errors, null, 2));
                } else {
                    console.error(JSON.stringify(error_messages, null, 2));
                }
                console.error('FAILURE');
            } else {
                console.log('SUCCESS');
            }
        });
}

function commandActionImportCsv(csv_file, options) {
    let client = createClient(options);
    let imp = IMPORTING[options.parent.importing];
    let verbose = options.parent.verbose;
    let coa = options.changeOfAuthorization;

    // TODO stack the tests console.log('Pinging');
    // TODO stack the tests let ping = await commandActionPing(options);
    // TODO stack the tests console.log('Pinged', ping);

    const csv = require('csv-parser');
    const fs = require('fs');
    const results = [];
    const stats = {
        "attempts": 0,
        "created": -1,
        "updated": -1,
        "replaced": -1,
        "exists": 0,
        "not_exists": 0,
        "skipped": 0,
        "errors": 0,
        "unknowns": 0,
        "min": -1,
        "max": -1,
        "duration": -1,
    };
    let rows = []; // async.eachOfLimit
    let start0 = Date.now();
   
    fs.createReadStream(csv_file)
        .pipe(csv({ strict: true }))
        .on('headers', (headers) => {
            // Catches EFBBBF (UTF-8 BOM) because the buffer-to-string
            // conversion translates it to FEFF (UTF-16 BOM)
            if (headers[0].charCodeAt(0) === 0xFEFF) {
                headers[0] = headers[0].slice(1);
                this.headers = headers;
            }
            let validated = validateHeaders(headers, options, imp);
            if (validated !== true) {
                console.error(validated);
                process.exit(1);
            }
        })
        .on('data', (data) => {
            // Memory intensive but the only current way to rate limit outbound calls.
            rows.push(data);
        })
        .on('end', () => {
            let strategy = options.strategy;
            let doing = 'Creating';
            let doCreate = false;
            let doUpdate = false;
            let doReplace = false;
            let orCreate = false;
            let orUpdate = false;
            let orReplace = false;
            if (strategy.startsWith('create')) {
                doCreate = true;
                doing = 'Creating';
                stats.created = 0;
                if (strategy.endsWith('update')) {
                    orUpdate = true;
                    stats.updated = 0;
                } else if (strategy.endsWith('replace')) {
                    orReplace = true;
                    stats.replaced = 0;
                }
            } else if (strategy.startsWith('update')) {
                doUpdate = true;
                doing = 'Updating';
                stats.updated = 0;
                if (strategy.endsWith('create')) {
                    orCreate = true;
                    stats.created = 0;
                } else if (strategy.endsWith('replace')) {
                    // N/A stats.replaced = 0;
                }
            } else if (strategy.startsWith('replace')) {
                doReplace = true;
                doing = 'Replacing';
                stats.replaced = 0;
                if (strategy.endsWith('update')) {
                    // N/A stats.updated = 0;
                } else if (strategy.endsWith('create')) {
                    orCreate = true;
                    stats.created = 0;
                }
            } else {
                console.error('Unknown strategy: ', strategy); // Impossible
            }

            stats.attempts = rows.length;
            // process 10 chunks at a time
            async.forEachOfLimit(rows, parseInt(options.sessions), (row, k, callback) => {
                row = normalizeData(row, options);
                let validated = validateData(row, options, imp);
                if (validated !== true) {
                    console.error(validated);
                    stats.skipped++;
                    callback();
                    return;
                }
                let key = row[imp.key];
                if (verbose) {
                    console.log(key + ' - ' + doing + '...', row);
                } else {
                    console.log(key + ' - ' + doing + '...');
                }
                if (!options.dryRun) {
                    if (doCreate) {
                        let do_callback = true;
                        let start = Date.now();
                        client[imp["CppmApiCreate"]+'Async'](row, coa)
                          .then((response) => {
                                let result = response.data;
                                if (response.status == 201) {
                                    let dur = Date.now() - start;
                                    console.log(key + ' - ' + 'Created: ' + result['id'] + ' in ' + dur + 'ms');
                                    if (verbose) {
                                        console.log(key, JSON.stringify(result, null, 2));
                                    }
                                    stats.created++;
                                    if (dur > stats.max) stats.max = dur;
                                    if (stats.min < 0 || (stats.min > 0 && dur < stats.min)) stats.min = dur;
                                } else {
                                    stats.unknowns++;
                                    console.error(key + ' - ERROR: ' + 'Unexpected status: ' + response.status, JSON.stringify(result, null, 2));
                                }
                          })
                          .catch((error) => {
                              let response = error.response;
                              if ((orUpdate || orReplace) && response && response.status == 422 && response.data && response.data.result && response.data.result.user_exists) {
                                  let doing = doUpdate ? 'Updating' : 'Replacing';
                                  console.log(key + ' - ' + 'Exists - ' + doing + '...');
                                  do_callback = false;
                                  let method = doUpdate ? imp["CppmApiUpdate"] : imp["CppmApiReplace"];
                                  let did = doUpdate ? 'Updated' : 'Replaced';
                                  client[method+'Async'](key, row, coa)
                                      .then((response) => {
                                          let result = response.data;
                                          if (response.status == 200) {
                                              let dur = Date.now() - start;
                                              console.log(key + ' - ' + did + ': ' + result['id'] + ' in ' + dur + 'ms');
                                              if (verbose) {
                                                  console.log(key, JSON.stringify(result, null, 2));
                                              }
                                              if (doUpdate) {
                                                  stats.updated++;
                                              } else {
                                                  stats.replaced++;
                                              }
                                              if (dur > stats.max) stats.max = dur;
                                              if (stats.min < 0 || (stats.min > 0 && dur < stats.min)) stats.min = dur;
                                          } else {
                                              stats.unknowns++;
                                              console.error(key + ' - ' + 'Unexpected status: ' + response.status, JSON.stringify(result, null, 2));
                                          }
                                      })
                                      .catch((error) => {
                                          handleAPIClientError(stats, key, error, verbose);
                                      })
                                      .finally(()=> {
                                          callback();
                                      });
                              } else {
                                  handleAPIClientError(stats, key, error, verbose);
                              }
                          })
                          .finally(()=> {
                              if (do_callback) {
                                  callback();
                              }
                          });
                    } else if (doUpdate || doReplace) {
                        let method = doUpdate ? imp["CppmApiUpdate"] : imp["CppmApiReplace"];
                        let did = doUpdate ? 'Updated' : 'Replaced';
                        let do_callback = true;
                        let start = Date.now();
                        client[method+'Async'](key, row, coa)
                          .then((response) => {
                                let result = response.data;
                                if (response.status == 200) {
                                    let dur = Date.now() - start;
                                    console.log(key + ' - ' + did + ': ' + result['id'] + ' in ' + dur + 'ms');
                                    if (verbose) {
                                        console.log(key, JSON.stringify(result, null, 2));
                                    }
                                    if (doUpdate) {
                                        stats.updated++;
                                    } else {
                                        stats.replaced++;
                                    }
                                    if (dur > stats.max) stats.max = dur;
                                    if (stats.min < 0 || (stats.min > 0 && dur < stats.min)) stats.min = dur;
                                } else {
                                    stats.unknowns++;
                                    console.error(key + ' - ' + 'Unknown status: ' + response.status, JSON.stringify(result, null, 2));
                                }
                          })
                          .catch((error) => {
                              let response = error.response;
                              if (orCreate && response && response.status == 404 && response.data && response.data.title && response.data.title == 'Not Found') {
                                  console.log(key + ' - ' + 'Not Found - Creating...');
                                  do_callback = false;
                                  client[imp["CppmApiCreate"]+'Async'](row, coa)
                                    .then((response) => {
                                          let result = response.data;
                                          if (response.status == 201) {
                                              let dur = Date.now() - start;
                                              console.log(key + ' - ' + 'Created: ' + result['id'] + ' in ' + dur + 'ms');
                                              if (verbose) {
                                                  console.log(key, JSON.stringify(result, null, 2));
                                              }
                                              stats.created++;
                                              if (dur > stats.max) stats.max = dur;
                                              if (stats.min < 0 || (stats.min > 0 && dur < stats.min)) stats.min = dur;
                                          } else {
                                              stats.unknowns++;
                                              console.error(key + ' - ' + 'Unknown status: ' + response.status, JSON.stringify(result, null, 2));
                                          }
                                    })
                                    .catch((error) => {
                                        handleAPIClientError(stats, key, error, verbose);
                                    })
                                    .finally(()=> {
                                        callback();
                                    });
                              } else {
                                  handleAPIClientError(stats, key, error, verbose);
                              }
                          })
                          .finally(()=> {
                              if (do_callback) {
                                  callback();
                              }
                          });
                    } else {
                        console.error(key + ' - ERROR: ' + 'Impossible to be here');
                        callback();
                    }
                } else {
                      callback();
                }
            }).finally(() => {
                stats.duration = Date.now() - start0;
                console.log('Results:', JSON.stringify(stats, null, 2));
            });
        });
}

program.parse(process.argv);
