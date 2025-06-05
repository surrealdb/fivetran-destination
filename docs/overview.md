---
name: SurrealDB
title: Fivetran destination for SurrealDB | Configuration and documentation
description: Move your data to SurrealDB using Fivetran.
menuPosition: 195
---

# SurrealDB {% badge text="Partner-Built" /%} {% availabilityBadge connector="surrealdb_destination" /%}

SurrealDB is a multi-model database that allows you to store data in a variety of ways. You can use SurrealDB as a graph, document, time-series, or vector database, all designed to be queried with the SurrealQL query language.

{% note %}
This destination is [partner-built](/docs/partner-built-program).
For any questions related to the self-hosted SurrealDB destination and its documentation, contact [SurrealDB Support](https://surrealdb.com/contact). For SurrealDB Cloud, refer to [Surreal Cloud Suppoort](https://surrealdb.com/docs/cloud/billing-and-support/support).
{% /note %}

------------------

## Setup guide

Follow our [step-by-step SurrealDB setup guide](/docs/destinations/surrealdb/setup-guide) to connect your Fivetran destination with SurrealDB.

------------------

## Type transformation and mapping

The [data types in SurrealDB](https://surrealdb.com/docs/surrealql/datamodel#data-types) follow Fivetran's [standard data type storage](/docs/destinations#datatypes).

We use the following data type conversions:

| Fivetran Data Type | SurrealDB Data Type |
| - | - |
| BOOLEAN | bool | 
| SHORT | int | 
| INT | int | 
| LONG | int | 
| FLOAT | float | 
| DOUBLE | float | 
| BIGDECIMAL | decimal | 
| LOCALDATE | datetime | 
| INSTANT | datetime | 
| LOCALDATETIME | datetime | 
| STRING | string | 
| JSON | object | 
| BINARY | bytes | 


------------------

## Limitations

- Even though `FLOAT` occupies 32 bits, SurrealDB stores it the same way as `DOUBLE` and uses the `float` data type that takes up a 64-bit space.
- The destination has not been tested for each and every source Fivetran supports. If you experience any issues, such as sync failures, contact [SurrealDB Support](https://surrealdb.com/contact) or [Surreal Cloud Suppoort](https://surrealdb.com/docs/cloud/billing-and-support/support).

------------------

## Optimized memory usage

The SurrealDB destination connector works best when it processes smaller transactions. If you encounter slow sync speeds, timeouts, or failling syncs when syncing large volumes of data, consider assigning more resources to the associated SurrealDB instance. Use a larger instance type if you're on Surreal Cloud or hosting the database on some other IaaS.
