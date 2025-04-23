---
name: SurrealDB Destination Overview
title: Fivetran destination for SurrealDB Overview
description: Move your data to SurrealDB using Fivetran.
hidden: false
---

# SurrealDB {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}

SurrealDB is a multi-model database that allows you to store data in a variety of ways. You can use SurrealDB as a Graph, Document, Time-Series, or Vector database, all designed to be queried with our SurrealQL query language. Fivetran supports ingesting data from any sources to SurrealDB via this connector.

> NOTE: This destination is [partner-built](/docs/partner-built-program).
> For any questions related to self-hosted SurrealDB destination and its documentation, contact [SurrealDB Support](https://surrealdb.com/contact). For SurrealDB Cloud, refer to [Surreal Cloud Suppoort](https://surrealdb.com/docs/cloud/billing-and-support/support).

------------------

## Setup guide

Follow our [step-by-step {Destination} setup guide](/docs/destinations/surrealdb/setup-guide) to connect your SurrealDB destination with Fivetran.

------------------

## Type transformation and mapping

The data types in your SurrealDB follow Fivetran's [standard data type storage](/docs/destinations#datatypes).

We use the following data type conversions:

| Fivetran Data Type | [SurrealDB Data Type](https://surrealdb.com/docs/surrealql/datamodel#data-types) | Notes |
| - | - | - |
| BOOLEAN | bool | |
| SHORT | int | |
| INT | int | |
| LONG | int | |
| FLOAT | float | |
| DOUBLE | float | |
| BIGDECIMAL | decimal | |
| LOCALDATE | datetime | |
| INSTANT | datetime | |
| LOCALDATETIME | datetime | |
| STRING | string | |
| JSON | object | |
| BINARY | bytes | |

See [SurrealDB Data Types](https://surrealdb.com/docs/surrealql/datamodel#data-types)
for all the available SurrealDB data types.

------------------

## Hybrid Deployment support

We do not support for the [Hybrid Deployment model](/docs/core-concepts/architecture/hybrid-deployment) for SurrealDB destinations for now.

------------------

## Limitations

- `FLOAT` from sources takes same space as `DOUBLE` in some sense, as SurrealDB offers `float` which is 64-bit
- We have not tested this for every source Fivetran supports. That said, there may be edge-cases that something that should work does not work. Please report us if you encounter any sync failures that shouldn't happen in theory.

------------------

## Optimize SurrealDB Destination

- The connector is designed carefully so that it balances the load on the connector and on the target SurrealDB. For example, it prefers smaller transactions over big ones, so that it does not consume large memory temporarily when syncing many items.
- That said, if you encounter sync slowness or timeouts while syncing, failing SurrealDB when syncing large volume of data, and so on, you should consider giving more resources to the target SurrealDB instance. Use larger instance types if you're on some IaaS. Use larger instance type if you're on Surreal Cloud.
