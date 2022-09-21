# Target Postgres

[![CircleCI](https://circleci.com/gh/datamill-co/target-postgres.svg?style=svg)](https://circleci.com/gh/datamill-co/target-postgres)

[![PyPI version](https://badge.fury.io/py/singer-target-postgres.svg)](https://pypi.org/project/singer-target-postgres/)

[![](https://img.shields.io/librariesio/github/datamill-co/target-postgres.svg)](https://libraries.io/github/datamill-co/target-postgres)

[![kandi X-Ray](https://kandi.openweaver.com/badges/xray.svg)](https://kandi.openweaver.com/python/datamill-co/target-postgres)

A [Singer](https://singer.io/) postgres target, for use with Singer streams generated by Singer taps.

## Features

- Creates SQL tables for [Singer](https://singer.io) streams
- Denests objects flattening them into the parent object's table
- Denests rows into separate tables
- Adds columns and sub-tables as new fields are added to the stream [JSON Schema](https://json-schema.org/)
- Full stream replication via record `version` and `ACTIVATE_VERSION` messages.

## Install

1. Add `libpq` dependency

```sh
# macos
brew install postgresql
```
```sh
# ubuntu
sudo apt install libpq-dev
```

1. install `singer-target-postgres`

```sh
pip install singer-target-postgres
```

## Usage

1. Follow the
   [Singer.io Best Practices](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)
   for setting up separate `tap` and `target` virtualenvs to avoid version
   conflicts.

1. Create a [config file](#configjson) at
   `~/singer.io/target_postgres_config.json` with postgres connection
   information and target postgres schema.

   ```json
   {
     "postgres_host": "localhost",
     "postgres_port": 5432,
     "postgres_database": "my_analytics",
     "postgres_username": "myuser",
     "postgres_password": "1234",
     "postgres_schema": "mytapname"
   }
   ```

1. Run `target-postgres` against a [Singer](https://singer.io) tap.

   ```bash
   ~/.virtualenvs/tap-something/bin/tap-something \
     | ~/.virtualenvs/target-postgres/bin/target-postgres \
       --config ~/singer.io/target_postgres_config.json >> state.json
   ```

   If you are running windows, the following is equivalent:

   ```
   venvs\tap-exchangeratesapi\Scripts\tap-exchangeratesapi.exe | ^
   venvs\target-postgresql\Scripts\target-postgres.exe ^
   --config target_postgres_config.json
   ```

### Config.json

The fields available to be specified in the config file are specified
here.

| Field                       | Type                  | Default                            | Details                                                                                                                                                                                                                                                                                                                                                                               |
| --------------------------- | --------------------- | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `postgres_host`             | `["string", "null"]`  | `"localhost"`                      |                                                                                                                                                                                                                                                                                                                                                                                       |
| `postgres_port`             | `["integer", "null"]` | `5432`                             |                                                                                                                                                                                                                                                                                                                                                                                       |
| `postgres_database`         | `["string"]`          | `N/A`                              |                                                                                                                                                                                                                                                                                                                                                                                       |
| `postgres_username`         | `["string", "null"]`  | `N/A`                              |                                                                                                                                                                                                                                                                                                                                                                                       |
| `postgres_password`         | `["string", "null"]`  | `null`                             |                                                                                                                                                                                                                                                                                                                                                                                       |
| `postgres_schema`           | `["string", "null"]`  | `"public"`                         |                                                                                                                                                                                                                                                                                                                                                                                       |
| `postgres_sslmode`          | `["string", "null"]`  | `"prefer"`                         | Refer to the [libpq](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS) docs for more information about SSL                                                                                                                                                                                                                                              |
| `postgres_sslcert`          | `["string", "null"]`  | `"~/.postgresql/postgresql.crt"`   | Only used if a SSL request w/ a client certificate is being made                                                                                                                                                                                                                                                                                                                      |
| `postgres_sslkey`           | `["string", "null"]`  | `"~/.postgresql/postgresql.key"`   | Only used if a SSL request w/ a client certificate is being made                                                                                                                                                                                                                                                                                                                      |
| `postgres_sslrootcert`      | `["string", "null"]`  | `"~/.postgresql/root.crt"`         | Used for authentication of a server SSL certificate                                                                                                                                                                                                                                                                                                                                   |
| `postgres_sslcrl`           | `["string", "null"]`  | `"~/.postgresql/root.crl"`         | Used for authentication of a server SSL certificate                                                                                                                                                                                                                                                                                                                                   |
| `invalid_records_detect`    | `["boolean", "null"]` | `true`                             | Include `false` in your config to disable `target-postgres` from crashing on invalid records                                                                                                                                                                                                                                                                                          |
| `invalid_records_threshold` | `["integer", "null"]` | `0`                                | Include a positive value `n` in your config to allow for `target-postgres` to encounter at most `n` invalid records per stream before giving up.                                                                                                                                                                                                                                      |
| `disable_collection`        | `["string", "null"]`  | `false`                            | Include `true` in your config to disable [Singer Usage Logging](#usage-logging).                                                                                                                                                                                                                                                                                                      |
| `logging_level`             | `["string", "null"]`  | `"INFO"`                           | The level for logging. Set to `DEBUG` to get things like queries executed, timing of those queries, etc. See [Python's Logger Levels](https://docs.python.org/3/library/logging.html#levels) for information about valid values.                                                                                                                                                      |
| `persist_empty_tables`      | `["boolean", "null"]` | `False`                            | Whether the Target should create tables which have no records present in Remote.                                                                                                                                                                                                                                                                                                      |
| `max_batch_rows`            | `["integer", "null"]` | `200000`                           | The maximum number of rows to buffer in memory before writing to the destination table in Postgres                                                                                                                                                                                                                                                                                    |
| `max_buffer_size`           | `["integer", "null"]` | `104857600` (100MB in bytes)       | The maximum number of bytes to buffer in memory before writing to the destination table in Postgres                                                                                                                                                                                                                                                                                   |
| `batch_detection_threshold` | `["integer", "null"]` | `5000`, or 1/40th `max_batch_rows` | How often, in rows received, to count the buffered rows and bytes to check if a flush is necessary. There's a slight performance penalty to checking the buffered records count or bytesize, so this controls how often this is polled in order to mitigate the penalty. This value is usually not necessary to set as the default is dynamically adjusted to check reasonably often. |
| `state_support`             | `["boolean", "null"]` | `True`                             | Whether the Target should emit `STATE` messages to stdout for further consumption. In this mode, which is on by default, STATE messages are buffered in memory until all the records that occurred before them are flushed according to the batch flushing schedule the target is configured with.                                                                                    |
| `add_upsert_indexes`        | `["boolean", "null"]` | `True`                             | Whether the Target should create column indexes on the important columns used during data loading. These indexes will make data loading slightly slower but the deduplication phase much faster. Defaults to on for better baseline performance.                                                                                                                                      |
| `before_run_sql`            | `["string", "null"]`  | `None`                             | Raw SQL statement(s) to execute as soon as the connection to Postgres is opened by the target. Useful for setup like `SET ROLE` or other connection state that is important.                                                                                                                                                                                                          |
| `after_run_sql`             | `["string", "null"]`  | `None`                             | Raw SQL statement(s) to execute as soon as the connection to Postgres is opened by the target. Useful for setup like `SET ROLE` or other connection state that is important.                                                                                                                                                                                                          |
| `before_run_sql_file`       | `["string", "null"]`  | `None`                             | Similar to `before_run_sql` but reads an external file instead of SQL in the JSON config file.                                                                                                                                                                                                                                                                                        |
| `after_run_sql_file`        | `["string", "null"]`  | `None`                             | Similar to `after_run_sql` but reads an external file instead of SQL in the JSON config file.                                                                                                                                                                                                                                                                                         |
| `application_name`        | `["string", "null"]`  | `None`                             | Set the postgresql `application_name` connection option to help with debugging, etc...                                                                                                                                                                                                                                                                                         |


### Supported Versions

`target-postgres` only supports [JSON Schema Draft4](http://json-schema.org/specification-links.html#draft-4).
While declaring a schema _is optional_, any input schema which declares a version
other than 4 will be rejected.

`target-postgres` supports all versions of PostgreSQL which are presently supported
by the PostgreSQL Global Development Group. Our [CI config](https://github.com/datamill-co/target-postgres/blob/master/.circleci/config.yml) defines all versions we are currently supporting.

| Version | Current minor | Supported | First Release      | Final Release     |
| ------- | ------------- | --------- | ------------------ | ----------------- |
| 12      | 12.2          | Yes       | October 3, 2019    | November 14, 2024 |
| 11      | 11.7          | Yes       | October 18, 2018   | November 9, 2023  |
| 10      | 10.12         | Yes       | October 5, 2017    | November 10, 2022 |
| 9.6     | 9.6.17       | Yes       | September 29, 2016 | November 11, 2021 |
| 9.5     | 9.5.21        | Yes       | January 7, 2016    | February 11, 2021 |
| 9.4     | 9.4.26        | Yes       | December 18, 2014  | February 13, 2020 |
| 9.3     | 9.3.25        | No        | September 9, 2013  | November 8, 2018  |

_The above is copied from the [current list of versions](https://www.postgresql.org/support/versioning/) on Postgresql.org_

## Known Limitations

- Requires a [JSON Schema](https://json-schema.org/) for every stream.
- Only string, string with date-time format, integer, number, boolean,
  object, and array types with or without null are supported. Arrays can
  have any of the other types listed, including objects as types within
  items.
  - Example of JSON Schema types that work
    - `['number']`
    - `['string']`
    - `['string', 'null']`
  - Exmaple of JSON Schema types that **DO NOT** work
    - `['string', 'integer']`
    - `['integer', 'number']`
    - `['any']`
    - `['null']`
- JSON Schema combinations such as `anyOf` and `oneOf` are not supported.
- JSON Schema \$ref is partially supported:
  - **_NOTE:_** The following limitations are known to **NOT** fail gracefully
  - Presently you cannot have any circular or recursive `$ref`s
  - `$ref`s must be present within the schema:
    - URI's do not work
    - if the `$ref` is broken, the behaviour is considered unexpected
- Any values which are the `string` `NULL` will be streamed to PostgreSQL as the literal `null`
- Table names are restricted to:
  - 63 characters in length
  - can only be composed of `_`, lowercase letters, numbers, `$`
  - cannot start with `$`
  - ASCII characters
- Field/Column names are restricted to:
  - 63 characters in length
  - ASCII characters

## Indexes

If the `add_upsert_indexes` config option is enabled, which it is by default, `target-postgres` adds indexes on the tables it creates for its own queries to be more performant. Specifically, `target-postgres` automatically adds indexes to the `_sdc_sequence` column and the `_sdc_level_<n>_id` columns which are used heavily when inserting and upserting.

`target-postgres` doesn't have any facilities for adding other indexes to the managed tables, so if there are more indexes required, they should be added by another downstream tool, or can just be added by an administrator when necessary. Note that these indexes incur performance overhead to maintain as data is inserted, These indexes can also prevent `target-postgres` from dropping columns in the future if the schema of the table changes, in which case an administrator should drop the index so `target-postgres` is able to drop the columns it needs to.

**Note**: Index adding is new as of version `0.2.1`, and `target-postgres` does not retroactively create indexes for tables it created before that time. If you want to add indexes to older tables `target-postgres` is loading data into, they should be added manually.

## Usage Logging

[Singer.io](https://www.singer.io/) requires official taps and targets to collect anonymous usage data. This data is only used in aggregate to report on individual tap/targets, as well as the Singer community at-large. IP addresses are recorded to detect unique tap/targets users but not shared with third-parties.

To disable anonymous data collection set `disable_collection` to `true` in the configuration JSON file.

## Developing

`target-postgres` utilizes [setup.py](https://python-packaging.readthedocs.io/en/latest/index.html) for package
management, and [PyTest](https://docs.pytest.org/en/latest/contents.html) for testing.

### Documentation

See also:

- [DECISIONS](./DECISIONS.md): A document containing high level explanations of various decisions and decision making paradigms. A good place to request more explanation/clarification on confusing things found herein.
- [TableMetadata](./docs/TableMetadata.md): A document detailing some of the metadata necessary for `TargetPostgres` to function correctly on the Remote

### Docker

If you have [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) installed, you can
easily run the following to get a local env setup quickly.

```sh
$ docker-compose up -d --build
$ docker logs -tf target-postgres_target-postgres_1 # You container names might differ
```

As soon as you see `INFO: Dev environment ready.` you can shell into the container and start running test commands:

```sh
$ docker-compose exec target-postgres bash
(target-postgres) root@...:/code# pytest
```

The environment inside the docker container has a virtualenv set up and activated, with an `--editable` install of `target-postgres` inside it and your local code mounted as a Docker volume. If you make changes on your host and re-run `pytest` any changes should be reflected inside the container.

See the [PyTest](#pytest) commands below!

### DB

To run the tests, you will need a PostgreSQL server running.

**_NOTE:_** Testing assumes that you've exposed the traditional port `5432`.

Make sure to set the following env vars for [PyTest](#pytest):

```sh
$ EXPORT POSTGRES_HOST='<your-host-name>' # Most likely 'localhost'
$ EXPORT POSTGRES_DB='<your-db-name>'     # We use 'target_postgres_test'
$ EXPORT POSTGRES_USER='<your-user-name'  # Probably just 'postgres', make sure this user has no auth
```

### PyTest

To run tests, try:

```sh
$ python setup.py pytest
```

If you've `bash` shelled into the Docker Compose container ([see above](#docker)), you should be able to simply use:

```sh
$ pytest
```

## Collaboration and Contributions

Join the conversation over at the [Singer.io Slack](singer-io.slack.com) and on the `#target-postgres` channel.

Try to adhere to the following for contributing:

- File New Issue -> Fork -> New Branch(If needed) -> Pull Request -> Approval -> Merge

Users can file an issue without submitting a pull request but be aware not all issues can or will be addressed.

## Sponsorship

Target Postgres is sponsored by Data Mill (Data Mill Services, LLC) [datamill.co](https://datamill.co/).

Data Mill helps organizations utilize modern data infrastructure and data science to power analytics, products, and services.

---

Copyright Data Mill Services, LLC 2018
