# clearpass-csv2api

Node CLI tool to import a CSV into ClearPass via the RESTful API.

Currently supports Guests and Devices.  Reference the built in documentation for help.

```
npm install -g clearpass-csv2api
```

```
$ clearpass-csv2api help
Usage: clearpass-csv2api [options] [command]

Options:
  -V, --version                 output the version number
  -v, --verbose                 Output extra debugging (default: false)
  --importing <importing>       What is being imported.  Current accepted values: device, guest (default: "device")
  -h, --help                    Display help for command

Commands:
  import [options] <csv_file>   Import a CSV of items into ClearPass
  testcsv [options] <csv_file>  Confirm the csv seems valid.
  ping [options]                Test connectivity, authentication and privileges
  generate [options] <count>    Generate a random CSV
  help [command]                display help for command

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
```

```
$ clearpass-csv2api help import
Usage: clearpass-csv2api import [options] <csv_file>

Import a CSV of items into ClearPass

Options:
  --host <host>                           The IP address/hostname of ClearPass. (default: "127.0.0.1")
  --insecure                              Disable SSL validation (default: false)
  --client_id <client_id>                 API Client ID (default: "Client1")
  --client_secret <client_secret>         API Client Secret (default: "")
  -x --extra <extra>                      An extra key=value pair. Multiple supported. (default: {})
  --exclude <list of columns to exclude>  Excluded list (default: [])
  --sessions <sessions>                   Max simultaneous sessions (default: 1)
  --strategy <strategy>                   Strategy to use when making API calls
      create-only       Create new items only.  Existing will be ignored.
      create-or-replace Create new items or replace existing entries with the CSV.
      create-or-update  Create new items or update existing entries with data in the CSV.
      update-only       Update existing items only.  Entries without a match will be skipped.
      update-or-create  Update existing items.  If the entry does not exist it will be created.
      replace-only      Replace existing items only.  Entries without a match will be skipped.
      replace-or-create Replace existing items.  If the entry does not exist it will be created.
       (default: "create-only")
  --change-of-authorization               Send RADIUS CoA requests (default: false)
  --dry-run                               Dry run, do not send any API (default: false)
  -h, --help                              Display help for command
```

```
$ clearpass-csv2api help testcsv
Usage: clearpass-csv2api testcsv [options] <csv_file>

Confirm the csv seems valid.

Options:
  -x --extra <extra>                      An extra key=value pair. Multiple supported. (default: {})
  --exclude <list of columns to exclude>  Excluded list (default: [])
  -h, --help                              Display help for command
```

```
$ clearpass-csv2api help ping
Usage: clearpass-csv2api ping [options]

Test connectivity, authentication and privileges

Options:
  --host <host>                    The IP address/hostname of ClearPass. (default: "127.0.0.1")
  --insecure                       Disable SSL validation (default: false)
  --client_id <client_id>          API Client ID (default: "Client1")
  --client_secret <client_secret>  API Client Secret (default: "")
  --insecure                       Disable SSL validation (default: false)
  -h, --help                       Display help for command

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
```

```
$ clearpass-csv2api help generate
Usage: clearpass-csv2api generate [options] <count>

Generate a random CSV

Options:
  -x --extra <extra>  An extra key=value pair. Multiple supported. (default: {})
  -h, --help          Display help for command
```

