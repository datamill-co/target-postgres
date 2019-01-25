# Changelog

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
