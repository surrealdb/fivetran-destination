---
name: SurrealDB Destination Setup Guide
title: Fivetran destination for SurrealDB Setup Guide
description: Follow the guide to set up SurrealDB as a destination.
---

# SurrealDB Destination Setup Guide {% badge text="Partner-Built" /%} {% availabilityBadge connector="surrealdb_destination" /%}

Follow our setup guide to connect SurrealDB to Fivetran.

{% note %}
This destination is [partner-built](/docs/partner-built-program).
For any questions related to self-hosted SurrealDB destination and its documentation, contact [SurrealDB Support](https://surrealdb.com/contact). For SurrealDB Cloud, refer to [Surreal Cloud Suppoort](https://surrealdb.com/docs/cloud/billing-and-support/support).
{% /note %}

-----

## Prerequisites

To connect SurrealDB to Fivetran, you need the following:

- A Fivetran role with the [Create Destinations or Manage Destinations](/docs/using-fivetran/fivetran-dashboard/account-settings/role-based-access-control#rbacpermissions) permissions.
- A SurrealDB token.
- A SurrealDB instance (self-hosted or Cloud) that is accessible by Fivetran.

---

## Setup instructions

### <span class="step-item">Get URL and token</span>

### Option 1: Self-hosted SurrealDB

1. For self-hosted SurrealDB, ensure your SurrealDB instance is accessible by Fivetran according to your Fivetran deployment:
    - For [Fivetran SaaS Deployment](/docs/deployment-models/saas-deployment), ensure your SurrealDB is accessible via Internet.
    - For [Fivetran Hybrid Deployment](/docs/deployment-models/hybrid-deployment), ensure your SurrealDB is accessible by the Fivetran Hybrid Deployment Agent.
    - For [Fivetran Self-Hosted Deployment](/docs/deployment-models/self-hosted-deployment), ensure your SurrealDB is accessible by the Fivetran HVR Agent.
2. Set up the token and use it following [SurrealDB's Authentication documentation](https://surrealdb.com/docs/surrealdb/security/authentication#token).

### Option 2: Surreal Cloud

1. Ensure your SurrealDB instance is up and running and accessible via Internet.
2. Browse the [Instances page](https://surrealist.app/cloud/instances) and select your chosen instance.
3. Click **Connect with Surreal CLI** and locate the `surreal sql --endpoint wss://YOUR_INSTANCE_HOSTNAME --token YOUR_TOKEN` command.
4. Run the command, and set up your own `ACCESS` or `USER`. The example below works for testing purposes:
    ```
    USE NS your_ns;
    USE DB your_db;
    DEFINE USER your_user ON DATABASE PASSWORD "YourPassword" ROLES OWNER;
    ```
5. Ensure the user/pass is working by running:
    ```
    surreal sql --endpoint wss://YOUR_INSTANCE_HOSTNAME --user your_user --pass YourPassword --ns your_ns --db your_db
    ```
6. Make a note of the `endpoint`, `user`, and `pass` parameters. You will need them to configure Fivetran.

7. (Optional) If you prefer using `token`, we recommend `DEFINE ACCESS ... TYPE JWT`. Refer to the [`DEFINE ACCESS > JWT` documentation](https://surrealdb.com/docs/surrealql/statements/define/access/jwt) to set up JWT access.
    - Verify if the token is working before proceeding to the next section, by running:
    ```
    surreal sql --endpoint wss://YOUR_INSTANCE_HOSTNAME --token your_token --ns your_ns --db your_db
    ```


### <span class="step-item"> Finish Fivetran configuration </span>

1. Log in to your [Fivetran account](https://fivetran.com/login).
2. Go to the **Destinations** page and click **Add destination**.
3. Enter a **Destination name** of your choice.
4. Click **Add**.
5. Select **SurrealDB** as the destination type.
6. Enter the `url`, `user` and `pass` (or `token`) you verified in the previous step.
   {% note %}
   The `url` setting corresponds to the `endpoint` parameter you verified in the previous step.
   {% /note %}
7. Click **Save & Test**.

Fivetran [tests and validates](#setuptests) the SurrealDB connection. Upon successfully completing the setup tests, you can sync your data using Fivetran connectors to the SurrealDB destination.

In addition, Fivetran automatically configures a [Fivetran Platform Connector](/docs/logs/fivetran-platform) to transfer the connector logs and account metadata to a schema in this destination. The Fivetran Platform Connector enables you to monitor your connectors, track your usage, and audit changes. The connector sends all these details at the destination level.

{% important %}
If you are an Account Administrator, you can manually add the Fivetran Platform Connector on an account level so that it syncs all the metadata and logs for all the destinations in your account to a single destination. If an account-level Fivetran Platform Connector is already configured in a destination in your Fivetran account, then we don't add destination-level Fivetran Platform Connectors to the new destinations you create.
{% /important %}


### Setup tests

Fivetran performs the following SurrealDB connection tests:

- The Database Connection test checks if we can connect to your SurrealDB database using the provided URL and token.

The test should complete in a few seconds if your Fivetran deployment can access the target SurrealDB instance.

---

## Related articles

[<i aria-hidden="true" class="material-icons">description</i> Destination Overview](/docs/destinations/surrealdb)

<b> </b>

<!--[<i aria-hidden="true" class="material-icons">assignment</i> Release Notes](/docs/destinations/surrealdb/changelog)

<b> </b>-->

[<i aria-hidden="true" class="material-icons">settings</i> API Destination Configuration](/docs/rest-api/api-reference/destinations/create-destination?service=surrealdb_destination)

<b> </b>

[<i aria-hidden="true" class="material-icons">home</i> Documentation Home](/docs/getting-started)
