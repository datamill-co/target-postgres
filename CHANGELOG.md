# Changelog

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
