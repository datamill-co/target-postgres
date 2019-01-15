# Decisions

This document is intended to provide clarity on many of the decisions/rationalizations
which exist inside of [Datamill's](https://datamill.co/) Target SQL project
for [Singer](https://singer.io).

## Principles

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
  - examples include:
    - `objects` (ie, `{'a': 1, 'b': 2 ...}`)
    - `array` (ie, `[1, 'a', 2, {4: False}]`)
    - `anyOf`
- Standard SQL does not support compositional elements, but rather data which is highly structured in potentially many related tables
- To overcome this, `target-sql` provides tooling which unpacks:
  - json `objects` into their parent record
  - json `arrays` as sub tables

```py
# Stream `FOO`
[
  {'nested_object': {
    'a': 1,
    'b': 2
   }
   'nested_array': [
     {'c': False, 'd': 'abc'},
     {'c': True, 'd': 'xyz'}
   ]
  }
]


# Results in:
## Table `foo`
[
  {'nested_object__a': 1,
   'nested_object__b': 2}
]

## Table `foo__nested_array`
[
  {'c': False, 'd': 'abc'},
  {'c': True, 'd': 'xyz'}
]

```

#### Why

- This approach is inspired by what Stitch Data takes with `object`/`array` de-nesting.
- The user experience for those using a SQL querying language is better for flat tables
  - as compared to something like [PostgreSQL's JSONB](https://www.postgresql.org/docs/9.4/datatype-json.html) support
- Data warehouses tend to prefer [denormalized](https://en.wikipedia.org/wiki/Denormalization) structures while operational databases prefer normalized structures. We normalize the incoming structure so the user can choose what to do with the normalized raw data. Also it's easy to access and transform later than JSON blobs.

### Column Type Mismatch

#### What

1. A field has been streamed to the remote target with type `integer`
1. A new field with the _same raw name_ as the remote column has been streamed but has type `boolean`
    - Data of type `boolean` cannot be placed into a column of type `integer`
1. `target-sql` has tooling which will:
    1. rename the original column to `original_field_name__i`
    1. make the renamed column `nullable`
    1. create a new column of name `original_field_name__b`
    1. stream new data to `original_field_name__b`
    - (to see a full list of type suffixes, please see: [`json_schema._shorthand_mappings`](https://github.com/datamill-co/target-postgres/blob/d626061d7a0e785f06b19589e1951637f2748262/target_postgres/json_schema.py#L283))

#### Why

***TL;DR:*** Instead of throwing a hard error and forcing users to do some manual
transformation _before_ streaming data through `target-sql`, we chose a "best
effort" approach to resolving the underlying error.

By renaming and migrating the column we:

- make the resulting structure in the database the same no matter whether we upload column `integer` _then_ column `boolean` or vice versa.
- users learn of dependent views/columns blocking a type change _early_

### Column Name Collision

#### What

1. Field of name `foo` is streamed
1. Field of name `FOO` is then streamed
1. Since both of these names canonicalize to the same result (ie, `foo`), we have a name collision
1. When attempting to `upsert_table`, `SQLInterface` has to handle name collisions. To do this, it attaches a unique suffix to the name which _caused the collision_, not the original
    - The suffix is an auto-incrementing numerical value

```py
# Field `foo` is streamed
# Field `FOO` is streamed

[
  {'foo': 1,
   'FOO': False,
   'fOo': 4.0}
]

# The resulting table will be:

[
  {'foo': 1,
   'foo__1': False,
   'foo__2': 4.0}
]

```

#### Why

***TL;DR:*** Instead of throwing a hard error and forcing users to do some manual
transformation _before_ streaming data through `target-sql`, we chose a "best
effort" approach to resolving the underlying error.

- While this means that _ordering_ of fields/actions matters in regards to the final remote structure, users can observe their remote structure simply
- Hashes have been used as suffixes in past, but it was determined that these were too confusing for end users. So while they allowed us to adhere to [principle](#principles) (1), it meant [principle](#principles) (2) was being ignored.
  - Additionally, we chose _not to_ prepend a numerical suffix to _all_ columns for the same reason. _Most_ users are not going to have name collisions, so instead of making the overall user experience worse, we chose to have a targeted solution to this particular edge case

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

- This approach is inspired by what Stitch Data takes with `object` de-nesting.
- The user experience for those using a SQL querying language is better for flat tables
  - as compared to something like [PostgreSQL's JSONB](https://www.postgresql.org/docs/9.4/datatype-json.html) support

### Arrays

#### What

- `Arrays` are unrolled as individual rows into a child table
- The table name is constructed as `parent_table__field`

#### Why

- This approach is inspired by what Stitch Data takes with `array` de-nesting.
- The user experience for those using a SQL querying language is better for flat tables
  - as compared to something like [PostgreSQL's JSONB](https://www.postgresql.org/docs/9.4/datatype-json.html) support

## Queries

### What

- When we write SQL at any given point, we have the option to use "latest" PostgreSQL features
- We opt for features available from PostgreSQL 8.4.22 forward
- We ***DO NOT*** support PostgreSQL 8.4.22
  - any features/bugs issues based on this will be weighed against this decision as far as effort to benefit

### Why

- Supporting multiple versions of PostgreSQL has _thus far_ been fairly straightforward by adhering to only query support available in the _oldest_ version of supported PostgreSQL
- By doing this, we only have one main code base, instead of many fractured versions which all employ the latest/greatest system functions/methods/tables/information schemas available
- By using 8.4.22, supporting [Redshift](https://github.com/datamill-co/target-redshift) is made simpler
  - Redshift was originally split from [PostgreSQL 8.0.2](https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-and-postgres-sql.html)
  - At some point, a _lot_ of work was done by AWS to make Redshift a "simple fork" of PostgreSQL 8.4
- We do not _support_ PostgreSQL 8.4 simply because PostgreSQL does not support it anymore
  - Our _only_ benefit to making 8.4 query language our target is Redshift
  - When a new supported version of PostgreSQL comes along, and we undertake the effort to support it herein, if supporting it is simpler to do by breaking 8.4, we will move the necessary logic to [target-redshift](https://github.com/datamill-co/target-redshift)
