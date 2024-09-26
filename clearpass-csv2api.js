#!/usr/bin/env node

const CppmApi = require('aruba-clearpass-api')
const async = require('async')

const IMPORTING =
{
  device: {
    key: 'mac',
    aliases: ['device', 'devices'],
    privilege_create: 'mac_create',
    privilege_edit: 'full-user-control',
    required: ['role_id'],
    CppmApiCreate: 'createDevice',
    CppmApiReplace: 'replaceDeviceByMac',
    CppmApiUpdate: 'updateDeviceByMac'
  },
  guest: {
    key: 'username',
    aliases: ['guest', 'guests', 'guestuser', 'guestusers'],
    privilege_create: 'create_user',
    privilege_edit: 'full-user-control',
    required: ['password', 'role_id'],
    CppmApiCreate: 'createGuest',
    CppmApiReplace: 'replaceGuestByUserName',
    CppmApiUpdate: 'updateGuestByUserName'
  }
  /* "endpoint": {
        "key": "mac",
        "aliases": ["endpoint", "endpoints"],
        "privilege_create": "cppm_endpoints",
        "privilege_edit": "cppm_endpoints",
        "required": [],
        "CppmApiCreate": "createEndpoint",
        "CppmApiReplace": "replaceEndpointByMac",
        "CppmApiUpdate": "updateEndpointByMac"
    } */
}

const { program } = require('commander')

program
  .version('1.0.0')
  .helpOption('-h, --help', 'Display help for command')
  .option('-v, --verbose', 'Output extra debugging', false)
  .option('--importing <importing>', 'What is being imported.  Current accepted values: ' + Object.keys(IMPORTING).join(', '), 'device') // Match names from API Explorer
  .on('option:verbose', function () {
    // if (this.opts().verbose) log.level = 'debug'
  })
  .on('option:importing', function () {
    const importing = this.opts().importing = this.opts().importing.toLowerCase()
    console.debug('importing', importing)
    let importingMatch = false
    for (const importKey in IMPORTING) {
      if (Object.prototype.hasOwnProperty.call(IMPORTING, importKey)) {
        if (importKey === importing) {
          importingMatch = true
          break
        }
        const imp = IMPORTING[importKey]
        for (let i = 0; i < imp.aliases.length; i++) {
          if (imp.aliases[i] === importing) {
            importingMatch = true
            this.opts().importing = importKey
            break
          }
        }
      }
      if (importingMatch) {
        break
      }
    }
    if (!importingMatch) {
      console.error('Invalid value for: --importing (Allowed: ' + Object.keys(IMPORTING).join(', ') + ')')
      process.exit(1)
    }
  })

// Main import function
program
  .command('import <csv_file>'/*, { isDefault: true } */)
  .description('Import a CSV of items into ClearPass')
  .option('--host <host>', 'The IP address/hostname of ClearPass.', '127.0.0.1')
  // .option('-p, --port <port>', 'Port override', 443)
  .option('--insecure', 'Disable SSL validation', false)
  .option('--token <token>', 'Generated Bearer token', '')
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
  .on('option:token', function () {
    // aruba-clearpass-api auto adds 'Bearer '
    if (this.opts().token.substring(0, 7).toLowerCase() === 'bearer ') {
      this.opts().token = this.opts().token.substring(7)
    }
  })
  .on('option:exclude', function () {
    this.opts().exclude = typeof (this.opts().exclude) === 'string' ? this.opts().exclude.split(',') : []
  })
  .on('option:strategy', function () {
    const strategies = [
      'create-only', // POST
      'create-or-replace', // POST-PUT
      'create-or-update', // POST-PATCH
      'update-only', // PATCH
      'update-or-create', // PATCH-POST
      'replace-only', // PUT
      'replace-or-create' // PUT-POST
    ]
    if (!strategies.includes(this.opts().strategy)) {
      console.error('Invalid value for: --strategy (Allowed: ' + strategies.join(', ') + ')')
      process.exit(1)
    }
  })
  .action(commandActionImportCsv)

// Takes the first two lines of the csv and outputs what would have been created.
program
  .command('testcsv <csv_file>'/*, { isDefault: true } */)
  .description('Confirm the csv seems valid.')
  .option('-x --extra <extra>', 'An extra key=value pair. Multiple supported.', collectPairs, {})
  .option('--exclude <list of columns to exclude>', 'Excluded list', [])
  .on('option:exclude', function () {
    this.opts().exclude = typeof (this.opts().exclude) === 'string' ? this.opts().exclude.split(',') : []
  })
  .action(commandActionTestCSV)

// Confirm connectivity settings and permissions
program
  .command('ping')
  .description('Test connectivity, authentication and privileges')
  .option('--host <host>', 'The IP address/hostname of ClearPass.', '127.0.0.1')
  // .option('-p, --port <port>', 'Port override', 443)
  .option('--token <token>', 'Generated Bearer token', '')
  .option('--client_id <client_id>', 'API Client ID', 'Client1')
  .option('--client_secret <client_secret>', 'API Client Secret', '')
  .option('--insecure', 'Disable SSL validation', false)
  .on('option:token', function () {
    // aruba-clearpass-api auto adds 'Bearer '
    if (this.opts().token.substring(0, 7).toLowerCase() === 'bearer ') {
      this.opts().token = this.opts().token.substring(7)
    }
  })
  .addHelpText('after', `
To override the port, pass --host <host>:<port>.

Pass either --client_id and --client_secret or simply --token.  Generate Access Token in the UI can be used to generate a short term token.

API Clients are created in Guest » Administration » API Services » API Clients.
You need the following set:
  * Operating Mode: ClearPass REST API
  * Operator Profile: A profile with sufficient privileges
  * Grant Type: Client Credentials
The Client ID and Client Secret are passed as arguments.  Ensure you protect the secret.

Privileges:
  * ALL  : API Services > Allow API Access
  * Devices  : Devices > Create New Device and Guest Manager > Full User Control
  * Guests   : Guest Manager > Create New Guest Account and Guest Manager > Full User Control
`)
  .action(commandActionPing)

// Generate a randomized csv
program
  .command('generate <count>')
  .description('Generate a random CSV')
  .option('-x --extra <extra>', 'An extra key=value pair. Multiple supported.', collectPairs, {})
  .action(commandActionGenerate)

program.addHelpText('after', `
Example calls:

  Confirm credentials and permissions:
  $ clearpass-csv2api ping --host=192.0.2.10 --client_id=Client1 --client_secret=asdfadsf

  Test the CSV:
  $ clearpass-csv2api testcsv devices.csv
  $ clearpass-csv2api --importing=guest testcsv guest.csv

  Initiate the import:
  $ clearpass-csv2api --verbose import --host 192.0.2.10 --client_id=Client1 --client_secret=asdfadsf devices.csv
  $ clearpass-csv2api --verbose import --host 192.0.2.10 --client_id=Client1 --client_secret=asdfadsf --strategy=update-or-create devices-sync.csv
  $ clearpass-csv2api --verbose --importing=guest import --host 192.0.2.10 --client_id=Client1 --client_secret=asdfadsf guests.csv
  $ clearpass-csv2api --verbose import --host 192.0.2.10 --client_id=Client1 --client_secret=asdfadsf --exclude=id,region -x "notes=Import from ACME" -x "visitor_company=IoT Corp" devices.csv

  Generate test data:
  $ clearpass-csv2api generate 100 > devices-100.csv
  $ clearpass-csv2api generate 100 -x role_id=1 -x "notes=Import from ACME" > devices-ACME-Contractors.csv
  $ clearpass-csv2api --importing=guest generate 100 > guests-100.csv
`)

/**
 * Need to accumulate -x --extra
 */
function collectPairs (value, previous) {
  const eq = value.indexOf('=')
  if (eq <= 0 || eq === value.length) {
    console.error("Expected 'key=value' pair: ", value)
    process.exit(1)
  }
  const key = value.substring(0, eq)
  const val = value.substring(eq + 1)
  if (/\W/.test(key)) {
    console.error("Key can only contain letters, numbers and underscore 'key=value' pair: ", key)
    process.exit(1)
  }
  previous[key] = val
  return previous
}

function createClient (options) {
  const client = new CppmApi({
    host: options.host,
    token: options.token,
    clientId: options.client_id,
    clientSecret: options.client_secret,
    sslValidation: !options.insecure
  })
  return client
}

function normalizeData (data, options) {
  let i = 0
  let len = options.exclude.length
  for (i = 0; i < len; i++) {
    delete data[options.exclude[i]]
  }
  Object.assign(data, options.extra)
  const booleans = ['enabled']
  len = booleans.length
  for (i = 0; i < len; i++) {
    if (typeof data[booleans[i]] === 'string') {
      const b = data[booleans[i]].toLowerCase()
      data[booleans[i]] = b === '1' ||
                b === 'true' ||
                b === 'yes' ||
                b === 'on'
    }
  }
  return data
}

function validateHeaders (headers, options, imp) {
  const missing = []
  if (!headers.includes(imp.key)) {
    missing.push(imp.key)
  }
  for (let i = 0; i < imp.required.length; i++) {
    if (!headers.includes(imp.required[i])) {
      missing.push(imp.required[i])
    }
  }
  if (missing.length > 0) {
    return 'The following headers are required: ' + missing.join(', ')
  }

  return true
}

function validateData (data, options, imp) {
  // Check required fields.  Skips when unset as that must be checked above.
  const missing = []
  let key = '?'
  if (typeof (data[imp.key]) !== 'undefined' && data[imp.key] === '') {
    missing.push(imp.key)
  } else {
    key = data[imp.key]
  }
  for (let i = 0; i < imp.required.length; i++) {
    if (typeof (data[imp.required[i]]) !== 'undefined' && data[imp.required[i]] === '') {
      missing.push(imp.required[i])
    }
  }
  if (missing.length > 0) {
    return key + ' - SKIPPING: ' + 'The following fields are empty: ' + missing.join(', ')
  }

  // NULL checks
  // Note if a row is missing a column, csv-parser outright skips it.  You will not get null.
  // That is indicative of worse problems though.

  return true
}

function handleAPIClientError (stats, key, error, verbose) {
  const response = error.response
  if (!response) {
    console.error(key + ' - ERROR: ' + 'Unknown error:', JSON.stringify(error, null, 2))
    stats.errors++
  } else if (response.status === 400 && response.data && response.data.title === 'invalid_client') {
    stats.errors++
    console.error(key + ' - ERROR: ' + 'Bad keys?:', response.data.detail)
  } else if (response.status === 404 && response.data && response.data.title === 'Not Found') {
    // Does not exists
    stats.not_exists++
    console.error(key + ' - ERROR: ' + (response.data.detail || 'Not found'))
  } else if (response.status === 422 && response.data && response.data.result && response.data.result.user_exists) {
    // Exists
    stats.exists++
    console.error(key + ' - ERROR: ' + (response.data.result.message || 'Already exists'))
  } else if (response.status === 422 && response.data) {
    stats.errors++
    console.error(key + ' - ERROR: ' + response.data.detail, response.data.validation_messages)
  } else {
    stats.errors++
    console.error(key + ' - ERROR: ' + 'Unknown status: ' + response.status, JSON.stringify(response.data, null, 2))
  }
}

async function commandActionPing (options, command) {
  // const verbose = command.parent.opts().verbose
  const client = createClient(options)
  client.getMyInfo(function (error, result, statusCode) {
    if (error) {
      let msg = error.message
      if (result && result.detail) {
        msg += ' (' + result.detail + ')'
      }
      console.error('FAILED: ' + msg)
    } else {
      if (statusCode === 200) {
        console.log('SUCCESS', result)

        client.getMyPrivileges(function (error, result, statusCode) {
          if (error) {
            let msg = error.message
            if (result && result.detail) {
              msg += ' (' + result.detail + ')'
            }
            console.error('FAILED: ' + msg)
          } else {
            if (statusCode === 200) {
              const imp = IMPORTING[command.parent.opts().importing]
              if (!Array.isArray(result.privileges)) {
                console.error('FAILED: ' + "Missing 'privileges'")
              } else {
                if (result.privileges.includes(imp.privilege_create)) {
                  console.log('SUCCESS', 'Create privilege confirmed')
                } else {
                  console.error('FAILED: ' + "Missing '" + imp.privilege_create + "' privilege")
                }
                if (result.privileges.includes(imp.privilege_edit)) {
                  console.log('SUCCESS', 'Edit privilege confirmed')
                } else {
                  console.error('FAILED: ' + "Missing '" + imp.privilege_edit + "' privilege")
                }
              }
            } else {
              console.warn('Unexpected code: ' + statusCode, JSON.stringify(result, null, 2))
            }
          }
        })
      } else {
        console.warn('Unexpected code: ' + statusCode, JSON.stringify(result, null, 2))
      }
    }
  })
}

/**
 * Generate random CSV.  Similar to other scripts, all logging is stderr and stdout will be the CSV itself.
 */
function commandActionGenerate (count, options, command) {
  // const verbose = command.parent.opts().verbose
  // const imp = IMPORTING[command.parent.opts().importing]
  count = parseInt(count)
  if (isNaN(count) || count <= 0) {
    console.error('Count must be an integer')
    process.exit(1)
  }

  let extraKeys = ''
  let extraValues = ''
  for (const extraKey in options.extra) {
    if (Object.prototype.hasOwnProperty.call(options.extra, extraKey)) {
      extraKeys += ',' + extraKey
      let extraValue = options.extra[extraKey]
      if (extraValue.replace(/ /g, '').match(/[\s,"]/)) {
        extraValue = '"' + extraValue.replace(/"/g, '""') + '"'
      }
      extraValues += ',' + extraValue
    }
  }

  if (command.parent.opts().importing === 'device') {
    const ceil = 16777215
    if (count > ceil) {
      console.error('Count must be less than %d', ceil)
      process.exit(1)
    }
    let prefix = Math.floor(Math.random() * ceil)
    const prefixStr = prefix.toString(16).toUpperCase()
    prefix *= 256 * 256 * 256
    let header = 'mac'
    const hasRoleId = options.extra.role_id
    if (!hasRoleId) {
      header += ',role_id'
    }
    const hasVisitorName = options.extra.visitor_name
    if (!hasVisitorName) {
      header += ',visitor_name'
    }
    console.log(header + extraKeys)
    for (let i = 0; i < count; i++) {
      let mac = prefix + i
      mac = mac.toString(16).toUpperCase()
      while (mac.length < 12) {
        mac = '0' + mac
      }
      mac = mac.match(/.{2}/g).join('-')
      let row = mac
      if (!hasRoleId) {
        row += ',3'
      }
      if (!hasVisitorName) {
        row += ',Device ' + prefixStr + ' and ' + i
      }
      console.log(row + extraValues)
    }
  } else if (command.parent.opts().importing === 'guest') {
    const ceil = 16777215 // Arbitrary for Guests
    if (count > ceil) {
      console.error('Count must be less than %d', ceil)
      process.exit(1)
    }
    const prefix = Math.floor(Math.random() * ceil)
    const prefixStr = prefix.toString(16).toUpperCase()

    console.log('username,password,role_id,visitor_name' + extraKeys)
    for (let i = 0; i < count; i++) {
      const username = prefix + '_' + String(i).padStart(6, '0')
      const password = username.split('').reverse().join('')
      console.log('%s,%s,%d,"%s"%s', username, password, 3, 'Device ' + prefixStr + ' and ' + i, extraValues)
    }
  } else {
    console.error('Importing type not yet supported in the generator: ', command.parent.opts().importing)
  }
}

function commandActionTestCSV (csvFile, options, command) {
  const csv = require('csv-parser')
  const fs = require('fs')
  const errors = {}
  const imp = IMPORTING[command.parent.opts().importing]
  const verbose = command.parent.opts().verbose
  let first = true

  fs.createReadStream(csvFile)
    .pipe(csv({ strict: true }))
    .on('headers', (headers) => {
      // Catches EFBBBF (UTF-8 BOM) because the buffer-to-string
      // conversion translates it to FEFF (UTF-16 BOM)
      if (headers[0].charCodeAt(0) === 0xFEFF) {
        headers[0] = headers[0].slice(1)
        this.headers = headers
      }
      const validated = validateHeaders(headers, options, imp)
      if (validated !== true) {
        if (errors[validated]) {
          errors[validated]++
        } else {
          errors[validated] = 1
        }
      }
    })
    .on('data', (data) => {
      // console.log(data);
      data = normalizeData(data, options)
      if (first) {
        first = false
        console.log('Review this structure for correctness:')
        console.log(JSON.stringify(data, null, 2))
      }
      const validated = validateData(data, options, imp)
      if (validated !== true) {
        if (errors[validated]) {
          errors[validated]++
        } else {
          errors[validated] = 1
        }
      }
    })
    .on('error', (error) => {
      if (errors[error.message]) {
        errors[error.message]++
      } else {
        errors[error.message] = 1
      }
      console.log('error', error.message) // TODO: end is only called when no on.error is called!!!
    })
    .on('end', () => { // TODO: end is only called when no on.error is called!!!
      const errorMessages = Object.keys(errors)
      if (errorMessages.length > 0) {
        console.error('The following errors were reported:')
        if (verbose) {
          console.error(JSON.stringify(errors, null, 2))
        } else {
          console.error(JSON.stringify(errorMessages, null, 2))
        }
        console.error('FAILURE')
      } else {
        console.log('SUCCESS')
      }
    })
}

function commandActionImportCsv (csvFile, options, command) {
  const client = createClient(options)
  const imp = IMPORTING[command.parent.opts().importing]
  const verbose = command.parent.opts().verbose
  const coa = options.changeOfAuthorization

  // TODO stack the tests console.log('Pinging');
  // TODO stack the tests let ping = await commandActionPing(options, command);
  // TODO stack the tests console.log('Pinged', ping);

  const csv = require('csv-parser')
  const fs = require('fs')
  const stats = {
    attempts: 0,
    created: -1,
    updated: -1,
    replaced: -1,
    exists: 0,
    not_exists: 0,
    skipped: 0,
    errors: 0,
    unknowns: 0,
    min: -1,
    max: -1,
    duration: -1
  }
  const rows = [] // async.eachOfLimit
  const start0 = Date.now()

  fs.createReadStream(csvFile)
    .pipe(csv({ strict: true }))
    .on('headers', (headers) => {
      // Catches EFBBBF (UTF-8 BOM) because the buffer-to-string
      // conversion translates it to FEFF (UTF-16 BOM)
      if (headers[0].charCodeAt(0) === 0xFEFF) {
        headers[0] = headers[0].slice(1)
        this.headers = headers
      }
      const validated = validateHeaders(headers, options, imp)
      if (validated !== true) {
        console.error(validated)
        process.exit(1)
      }
    })
    .on('data', (data) => {
      // Memory intensive but the only current way to rate limit outbound calls.
      rows.push(data)
    })
    .on('end', () => {
      const strategy = options.strategy
      let doing = 'Creating'
      let doCreate = false
      let doUpdate = false
      let doReplace = false
      let orCreate = false
      let orUpdate = false
      let orReplace = false
      if (strategy.startsWith('create')) {
        doCreate = true
        doing = 'Creating'
        stats.created = 0
        if (strategy.endsWith('update')) {
          orUpdate = true
          stats.updated = 0
        } else if (strategy.endsWith('replace')) {
          orReplace = true
          stats.replaced = 0
        }
      } else if (strategy.startsWith('update')) {
        doUpdate = true
        doing = 'Updating'
        stats.updated = 0
        if (strategy.endsWith('create')) {
          orCreate = true
          stats.created = 0
        } else if (strategy.endsWith('replace')) {
          // N/A stats.replaced = 0;
        }
      } else if (strategy.startsWith('replace')) {
        doReplace = true
        doing = 'Replacing'
        stats.replaced = 0
        if (strategy.endsWith('update')) {
          // N/A stats.updated = 0;
        } else if (strategy.endsWith('create')) {
          orCreate = true
          stats.created = 0
        }
      } else {
        console.error('Unknown strategy: ', strategy) // Impossible
      }

      stats.attempts = rows.length
      // process 10 chunks at a time
      async.forEachOfLimit(rows, parseInt(options.sessions), (row, k, callback) => {
        row = normalizeData(row, options)
        const validated = validateData(row, options, imp)
        if (validated !== true) {
          console.error(validated)
          stats.skipped++
          callback()
          return
        }
        const key = row[imp.key]
        if (verbose) {
          console.log(key + ' - ' + doing + '...', row)
        } else {
          console.log(key + ' - ' + doing + '...')
        }
        if (!options.dryRun) {
          if (doCreate) {
            let doCallback = true
            const start = Date.now()
            client[imp.CppmApiCreate + 'Async'](row, coa)
              .then((response) => {
                const result = response.data
                if (response.status === 201) {
                  const dur = Date.now() - start
                  console.log(key + ' - ' + 'Created: ' + result.id + ' in ' + dur + 'ms')
                  if (verbose) {
                    console.log(key, JSON.stringify(result, null, 2))
                  }
                  stats.created++
                  if (dur > stats.max) stats.max = dur
                  if (stats.min < 0 || (stats.min > 0 && dur < stats.min)) stats.min = dur
                } else {
                  stats.unknowns++
                  console.error(key + ' - ERROR: ' + 'Unexpected status: ' + response.status, JSON.stringify(result, null, 2))
                }
              })
              .catch((error) => {
                const response = error.response
                if ((orUpdate || orReplace) && response && response.status === 422 && response.data && response.data.result && response.data.result.user_exists) {
                  const doing = orUpdate ? 'Updating' : 'Replacing'
                  console.log(key + ' - ' + 'Exists - ' + doing + '...')
                  doCallback = false
                  const method = orUpdate ? imp.CppmApiUpdate : imp.CppmApiReplace
                  const did = orUpdate ? 'Updated' : 'Replaced'
                  client[method + 'Async'](key, row, coa)
                    .then((response) => {
                      const result = response.data
                      if (response.status === 200) {
                        const dur = Date.now() - start
                        console.log(key + ' - ' + did + ': ' + result.id + ' in ' + dur + 'ms')
                        if (verbose) {
                          console.log(key, JSON.stringify(result, null, 2))
                        }
                        if (orUpdate) {
                          stats.updated++
                        } else {
                          stats.replaced++
                        }
                        if (dur > stats.max) stats.max = dur
                        if (stats.min < 0 || (stats.min > 0 && dur < stats.min)) stats.min = dur
                      } else {
                        stats.unknowns++
                        console.error(key + ' - ' + 'Unexpected status: ' + response.status, JSON.stringify(result, null, 2))
                      }
                    })
                    .catch((error) => {
                      handleAPIClientError(stats, key, error, verbose)
                    })
                    .finally(() => {
                      callback()
                    })
                } else {
                  handleAPIClientError(stats, key, error, verbose)
                }
              })
              .finally(() => {
                if (doCallback) {
                  callback()
                }
              })
          } else if (doUpdate || doReplace) {
            const method = doUpdate ? imp.CppmApiUpdate : imp.CppmApiReplace
            const did = doUpdate ? 'Updated' : 'Replaced'
            let doCallback = true
            const start = Date.now()
            client[method + 'Async'](key, row, coa)
              .then((response) => {
                const result = response.data
                if (response.status === 200) {
                  const dur = Date.now() - start
                  console.log(key + ' - ' + did + ': ' + result.id + ' in ' + dur + 'ms')
                  if (verbose) {
                    console.log(key, JSON.stringify(result, null, 2))
                  }
                  if (doUpdate) {
                    stats.updated++
                  } else {
                    stats.replaced++
                  }
                  if (dur > stats.max) stats.max = dur
                  if (stats.min < 0 || (stats.min > 0 && dur < stats.min)) stats.min = dur
                } else {
                  stats.unknowns++
                  console.error(key + ' - ' + 'Unknown status: ' + response.status, JSON.stringify(result, null, 2))
                }
              })
              .catch((error) => {
                const response = error.response
                if (orCreate && response && response.status === 404 && response.data && response.data.title && response.data.title === 'Not Found') {
                  console.log(key + ' - ' + 'Not Found - Creating...')
                  doCallback = false
                  client[imp.CppmApiCreate + 'Async'](row, coa)
                    .then((response) => {
                      const result = response.data
                      if (response.status === 201) {
                        const dur = Date.now() - start
                        console.log(key + ' - ' + 'Created: ' + result.id + ' in ' + dur + 'ms')
                        if (verbose) {
                          console.log(key, JSON.stringify(result, null, 2))
                        }
                        stats.created++
                        if (dur > stats.max) stats.max = dur
                        if (stats.min < 0 || (stats.min > 0 && dur < stats.min)) stats.min = dur
                      } else {
                        stats.unknowns++
                        console.error(key + ' - ' + 'Unknown status: ' + response.status, JSON.stringify(result, null, 2))
                      }
                    })
                    .catch((error) => {
                      handleAPIClientError(stats, key, error, verbose)
                    })
                    .finally(() => {
                      callback()
                    })
                } else {
                  handleAPIClientError(stats, key, error, verbose)
                }
              })
              .finally(() => {
                if (doCallback) {
                  callback()
                }
              })
          } else {
            console.error(key + ' - ERROR: ' + 'Impossible to be here')
            callback()
          }
        } else {
          callback()
        }
      }).finally(() => {
        stats.duration = Date.now() - start0
        console.log('Results:', JSON.stringify(stats, null, 2))
      })
    })
}

program.parse(process.argv)
