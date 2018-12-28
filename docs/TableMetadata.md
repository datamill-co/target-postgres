# Table Metadata

`SQLInterface` relies upon more schema information than is normally able to be
provided by a raw SQL schema. For instance, information about the original
non-canonicalized name of a field gets lost when that field is normalized into
a column.

To achieve this, metadata is stored. For Target Postgres, this metadata is currently
stored in a JSON Blob which is set onto each table's comment.

This document details the structure of this structure.  

## Table Comment Schema

| Field | Type | Default | Details |
| ----- | ---- | ------- | ------- |
| `version` |`["string", "null"]` | `null` | The Singer table version to be used with `activate_version` |
| `key_properties` | `["array", "null"]` | `null` | Array of `string`s representing the pks for the table. |
| `mappings` | `["object", "null"]`|  `null` | Mappings which take `current_column_name` to a `COLUMN_MAPPING` detailed below. |
| `table_mappings` | `{'type': ["array", "null"], 'items': {'type': "$TABLE_MAPPING"}}`|  `null` | Mappings which detail information about tables and their names. See `TABLE_MAPPING` below. |

## COLUMN_MAPPING

| Field | Type | Default | Details |
| ----- | ---- | ------- | ------- |
| `from` | `["string"]` | `N/A` | The original name of the field/property this column represents |
| `type` | `["array"]` | `N/A` | The `json_schema.type` of the `from` column |

## TABLE_MAPPING

| Field | Type | Default | Details |
| ----- | ---- | ------- | ------- |
| `type` | `["string"]` | `TABLE` | The type of mapping present, which is always `TABLE` |
| `from` | `{'type': ["array"], 'items': {'type': ["string"]}}` | `[]` | The fields/properties which lead to this (sub)table in the original schema. ie, the root table's path will always be `[<stream-name>]`, a table made from an array found at the property `foo` will be `[<stream-name>, "foo"]` etc. etc. |
| `to` | `["string"]` | `N/A` | The table name which takes the `from` path `to` the target's representation |
