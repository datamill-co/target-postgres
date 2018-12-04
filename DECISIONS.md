# Decisions

This document is intended to provide clarity on many of the decisions/rationalizations
which exist inside of [Datamill's](https://datamill.co/) Target SQL project
for [Singer](https://singer.io).

The guiding principles we try to adhere to herein as far as _how_ to reach a
conclusion are:

1. When possible, make the resulting data/schema in the remote target consistent, no matter the ordering of potential messages
  - ie, if our decision would result in a random schema being produced in the remote target for no reasonable benefit, this is in violation
1. Do right by the common _majority_ of users
1. Make a best effort to prevent a user from having to intervene
1. Use [Stitch’s offering and documentation](https://www.stitchdata.com/docs) as best practice guidance

## Schema

### De-nesting

#### What

- [JSON Schema](https://json-schema.org/) allows for complex schemas which have non-literal (ie, compositional) elements
- Standard SQL does not support compositional elements, but rather demands something closer to full denormalization
- To overcome this, `target-sql` provides tooling which unpacks json `objects` into their parent record, and json `arrays` as sub tables

#### Why

This is the same approach that Stitch Data takes with `array` de-nesting.

### Column Type Mismatch

#### What

- A field has been streamed to the remote target with type `integer`
- A new field with the _same raw name_ as the remote column has been streamed but has type `BAR`
- Data of type `BAR` cannot be placed into a column of type `boolean`
- `target-sql` has tooling which will:
  - rename the original column to `original_field_name__i`
  - make the renamed column `nullable`
  - create a new column of name `original_field_name__b`
  - stream new data to `original_field_name__b`
  - (to see a full list of type suffixes, please see: [`json_schema._shorthand_mappings`](https://github.com/datamill-co/target-postgres/blob/master/target_postgres/json_schema.py#L283))

#### Why

***TL;DR:*** Instead of throwing a hard error and forcing users to do some manual
transformation _before_ streaming data through `target-sql`, we chose a "best
effort" approach to resolving the underlying error.

By renaming and migrating the column we:

- make the resulting structure in the database the same no matter whether we upload column `integer` _then_ column `boolean` or vice versa.
- users learn of dependent views/columns blocking a type change _early_

### Column Name Collision

#### What

- When attempting to `upsert_table`, `SQLInterface` has to handle name

#### Why

***TL;DR:*** Instead of throwing a hard error and forcing users to do some manual
transformation _before_ streaming data through `target-sql`, we chose a "best
effort" approach to resolving the underlying error.



### Column Name Length

#### What

- `SQLInterface` provides a single field called `IDENTIFIER_FIELD_LENGTH` which is to be overridden by the implementing class
- Any column which is found to be excess of `IDENTIFIER_FIELD_LENGTH` is truncated to be no longer than `IDENTIFIER_FIELD_LENGTH`
- All `collision` and `type` information is preserved in the truncation
  - ie, any values which are suffixed onto the name as `__...`
- All original field/column names are preserved as a `column_mapping`

#### Why

***TL;DR:*** Instead of throwing a hard error and forcing users to do some manual
transformation _before_ streaming data through `target-sql`, we chose a "best
effort" approach to resolving the underlying error.

Most (all?) SQL targets we have encountered have length restrictions for identifiers
in their schema. Since arbitrary JSON _does_ not have this same restriction, we needed
a best effort mechanism for handling names which were either auto-generated and are
too long, or user input fields which physically cannot fit into the remote target.

As such, we chose to take the simplest method here for clarity. ie, truncate the
original/generated name, and then proceed with collision support as normal.

The implementing class is tasked with providing `canonicalize_identifier`, a method
which when called is expected to _only_ transform a string identifier into another
string identifier which contains only characters which are allowed by the remote target.

## Data De-nesting

### Objects

#### What

- `Objects` are unpacked into their parent table.
- The unpacked fields are prefixed with the name of the `field` which originally contained the object.

#### Why

This approach is inspired by what Stitch Data takes with `object` de-nesting.

### Arrays

#### What

- `Arrays` are unrolled as individual rows into a child table
- The table name is constructed as `parent_table__field`

#### Why

This approach is inspired by what Stitch Data takes with `array` de-nesting.
