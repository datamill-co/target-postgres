# Changelog

## 0.2.4

- **BUG FIX:** `multipleOf` validation
  - [FIX LINK](https://github.com/datamill-co/target-postgres/pull/179)
  - Due to floating point errors in Python and JSONSchema, `multipleOf`
    validation has been failing.

## 0.2.3

- **FEATURES:**
  - [`JSONSchema: anyOf` Support](https://github.com/datamill-co/target-postgres/pull/155)
    - Streamed `JSONSchema`s which include `anyOf` combinations should now be fully supported
    - This allows for full support of Stitch/Singer's `DateTime` string fallbacks.
  - [`JSONSchema`: allOf` Support](https://github.com/datamill-co/target-postgres/pull/154)
    - Streamed `JSONSchema`s which include `allOf` combinations should now be fully supported
    - Columns are persisted as normal.
    - This is _perceived_ to be most useful for merging objects, and putting in place things like `maxLength` etc.
- **BUG FIX:** Buffer Flushing at frequent intervals/with small batches
  - [FIX LINK](https://github.com/datamill-co/target-postgres/pull/169)
  - Buffer _size_ calculations relied upon some "sophisticated" logic for determining the "size" in
    memory of a Python object
  - The method used by Singer libraries is to simply use the size of the streamed `JSON` blob
  - Performance Improvement seen due to batches now being far larger and interactions with the remote
    being far fewer.
- **BUG FIX:** `NULLABLE` not being _implied_ when field is missing from streamed `JSONSchema`
  - [FIX LINK](https://github.com/datamill-co/target-postgres/pull/174)
  - If a field was persisted in remote, but then left _out_ of a subsequent streamed `JSONSchema`, we would fail
  - In this instance, the field is _implied_ to be `NULL`, but additionally, if values _are_ present for it
    in the streamed data, we _should_ persist it.

## 0.2.2

- **FEATURES:**
  - [Performance improvement for upserting data](https://github.com/datamill-co/target-postgres/pull/161)
    - Saw long running queries for some `SELECT COUNT(1)...` queries
      - Resulting in full table scans
    - These queries are _only_ being used for `is_table_empty`, therefore we can use a more efficient
      `SELECT EXISTS(...)` query which only needs a single row to be fetched

## 0.2.1

- **FEATURES:**
  - [Performance improvement for upserting data ](https://github.com/datamill-co/target-postgres/pull/152)
    - For large or even reasonably sized tables, trying to upsert the data was prohibitively slow
    - To mitigate this, we now add indexes to allow
    - This change can be opted out of via the `add_upsert_indexes` config option
    - **NOTE**: This only effects intallations post `0.2.1`, and will not upgrade/migrate existing installations
  - Support for latest PostgreSQL 12.0
    - PostgreSQL recently released 12.0, and we now have testing around it and can confirm that `target-postgres`
      _should_ function correctly for it!
- **BUG FIX:** `STATE` messages being sent at the wrong time
  - [FIX LINK](https://github.com/datamill-co/target-postgres/pull/149)
  - `STATE` messages were being output incorrectly for feeds which had many streams outputting at varying rates

## 0.2.0

- **NOTE:** The `minor` version bump is not expected to have much effect on folks. This was done to signal the
  output change from the below bug fix. It is our impression not many are using this feature yet anyways. Since
  this was _not_ a `patch` change, we decided to make this a `minor` instead of `major` change to raise _less_
  concern. Thank you for your patience!
- **FEATURES:**
  - [Performance improvement for creating `tmp` tables necessary for uploading data](https://github.com/datamill-co/target-postgres/pull/147)
    - PostgreSQL dialects allow for creating a table identical to a parent table in a single command
    - [`CREATE TABLE <name> (LIKE <parent-name>);`](https://www.postgresql.org/docs/9.1/sql-createtable.html)
    - Previously we leveraged using our `upsert` helpers to create new tables. This resulted in _many_ calls
      to remote, of varying complexity.
- **BUG FIX:** No `STATE` Message Wrapper necessary
  - [FIX LINK](https://github.com/datamill-co/target-postgres/pull/142)
  - `STATE` messages are formatted as `{"value": ...}`
  - `target-potgres` emitted the _full_ message
  - The official `singer-target-template`, doesn't write out that `value` "wrapper", and just writes
    the JSON blob contained in it
  - This fix makes `target-postgres` do the same

## 0.1.11

- **BUG FIX:** `canonicalize_identifier` Not called on _all_ identifiers persisted to remote
  - [FIX LINK](https://github.com/datamill-co/target-postgres/pull/144)
  - Presently, on column splits/name collisions, we add a suffix to an identifier
  - Previously, we did not canonicalize these suffixes
  - While this was not an issue for any `targets` currently in production, it was an issue
    for some up and coming `targets`.
  - This fix simply makes sure to call `canonicalize_identifier` before persisting an identifier to remote

## 0.1.10

- **FEATURES:**
  - [Root Table Name Canonicalization](https://github.com/datamill-co/target-postgres/pull/131)
    - The `stream` name is used for the value of the root table name in Postgres
    - `stream` names are controlled exclusively by the tap and do _not_ have to meet many standards
    - Previously, only `stream` names which were lowercase, alphanumeric, etc.
    - Now, the `target` can canonicalize the root table name, allowing for the input `stream` name to be
      whatever the `tap` provides.

## 0.1.9

- **Singer-Python:** bumped to latest _5.6.1_
- **Psycopg2:** bumped to latest _2.8.2_
- **FEATURES:**
  - [`STATE` Message support](https://github.com/datamill-co/target-postgres/pull/130)
    - Emits message only when all records buffered _before_ the `STATE` message have been persisted to remote.
  - [SSL Support for Postgres](https://github.com/datamill-co/target-postgres/pull/124)
    - Added config options for enabling/supporting SSL support.
- **BUG FIX:** `ACTIVATE_VERSION` Messages did not flush buffer
  - [FIX LINK](https://github.com/datamill-co/target-postgres/pull/135)
  - When we issue an activate version record, we presently do not flush the buffer after writing the batch. This results in more records being written to remote than need to be.
  - This results in no functionality change, and should not alleviate any _known_ bugs.
  - This should be purely performance related.

## 0.1.8

- **Singer-Python:** bumped to latest
- **Minor housekeeping:**
  - Updated container versions to latest
  - Updated README to reflect new versions of PostgreSQL Server

## 0.1.7

- **BUG FIX:** A bug was identified for de-nesting.
  - [ISSUE LINK](https://github.com/datamill-co/target-postgres/issues/109)
  - [FAILING TESTS LINK](https://github.com/datamill-co/target-postgres/pull/110)
  - [FIX LINK](https://github.com/datamill-co/target-postgres/pull/111)
  - Subtables with subtables did not serialize column names correctly
    - The column names ended up having the _table names_ (paths) prepended on them
    - Due to the denested table _schema_ and denested _records_ being different
      no information showed up in remote.
    - This bug was ultimately tracked down to the core denesting logic.
  - This will fix failing uploads which had **_nullable_** columns in subtables but
    no data was seen populating those columns.
    - The broken schema columns will still remain
  - Failing schemas which had **_non-null_** columns in subtables will still be broken
    - To fix will require dropping the associated tables, potentially resetting the entire
      `db`/`schema`

## 0.1.6

- **BUG FIX:** A bug was identified for path to column serialization.
  - [LINK](https://github.com/datamill-co/target-postgres/pull/100)
  - A nullable properties which had _multiple_ JSONSchema types
    - ie, something like `[null, string, integer ...]`
    - Failed to find an appropriate column in remote to persist `None` values to.
  - Found by usage of the [Hubspot Tap](https://github.com/singer-io/tap-hubspot)

## 0.1.5

- **FEATURES:**
  - [Added the `persist_empty_tables`](https://github.com/datamill-co/target-postgres/pull/97) config option which allows the Target to create empty tables in Remote.

## 0.1.4

- **BUG FIX:** A bug was identified in 0.1.3 with stream `key_properties` and canonicalization.
  - [LINK](https://github.com/datamill-co/target-postgres/pull/95)
  - Discovered and fixed by @mirelagrigoras
  - If the `key_properties` for a stream changed due to canonicalization, the stream would fail to persist due to:
    - the `persist_csv_rows` `key_properties` values would remain un-canonicalized (sp?) and therefore cause issues once serialized into a SQL statement
    - the pre-checks for tables would break because no values could be pulled from the schema with un-canonicalized fields pulled out of the `key_properties`
  - **NOTE:** the `key_properties` metadata is saved with _raw_ field names.

## 0.1.3

- **SCHEMA_VERSION: 1**
  - [LINK](https://github.com/datamill-co/target-postgres/pull/89)
  - Initialized a new field in remote table schemas `schema_version`
  - A migration in `PostgresTarget` handles updating this
- **BUG FIX:** A bug was identified in 0.1.2 with column type splitting.
  - [LINK](https://github.com/datamill-co/target-postgres/pull/89)
  - A schema with a field of type `string` is persisted to remote
    - Later, the same field is of type `date-time`
      - The values for this field will _not_ be placed under a new column, but rather under the original `string` column
  - A schema with a field of type `date-time` is persisted to remote
    - Later, the same field is of type `string`
      - The original `date-time` column will be made `nullable`
      - The values for this field will fail to persist
- **FEATURES:**
  - [Added the `logging_level`](https://github.com/datamill-co/target-postgres/pull/92) config option which uses standard Python Logger Levels to configure more details about what Target-Postgres is doing
    - Query level logging and timing
    - Table schema changes logging and timing
