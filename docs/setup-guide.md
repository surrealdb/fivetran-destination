---
name: SurrealDB Destination Setup Guide
title: Fivetran destination for SurrealDB Setup Guide
description: Follow the guide to set up SurrealDB as a destination.
hidden: false
---

# SurrealDB Destination Setup Guide {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}

Follow our setup guide to connect SurrealDB to Fivetran.

-----

## Prerequisites

To connect a SurrealDB to Fivetran, you need the following:

- A Fivetran role with the [Create Destinations or Manage Destinations](/docs/using-fivetran/fivetran-dashboard/account-settings/role-based-access-control#rbacpermissions) permissions
- A SurrealDB token
- SurrealDB instance (self-hosted or on Cloud) accessible by Fivetran

---

## Setup instructions

### <span class="step-item">Getting the URL and the token</span>

### Self-Hosted SurrealDB

1. Ensure your SurrealDB instance is accessible by Fivetran
  - For self-hosted SurrealDB, ensure your SurrealDB instance is accessible by Fivetran according to your Fivetran deployment:
    - For [Fivetran SaaS Deployment](https://fivetran.com/docs/deployment-models/saas-deployment), ensure your SurrealDB is
        accessible via the Internet.
    - For [Fivetran Hybrid Deployment](https://fivetran.com/docs/deployment-models/hybrid-deployment), ensure your SurrealDB is accessible by the Fivetran Hybrid Deployment Agent.
    - For [Fivetran Self-Hosted Deployment](https://fivetran.com/docs/deployment-models/self-hosted-deployment), ensure your SurrealDB is accessible by the Fivetran HVR Agent.
2. Set up the token and grab it following [SurrealDB Authentication documentation](https://surrealdb.com/docs/surrealdb/security/authentication#token).

### Surreal Cloud

1. Ensure your SurrealDB instance is up and running.
  - Any Fivetran Deployment will communicate with your Surreal Cloud instance over the Internet.
2. Browse [the Instances page](https://surrealist.app/cloud/instances) and click one of your instances
3. Click "Connect with Surreal CLI" and locate the command like `surreal sql --endpoint wss://YOUR_INSTANCE_HOSTNAME --token YOUR_TOKEN`

Take notes of `wss://YOUR_INSTANCE_HOSTNAME` and `YOUR_TOKEN` in the shown command. You use those for Fivetran connector configuration's `url` and `token` settings, respectively.

### <span class="step-item"> Complete Fivetran configuration </span>

{Required}
1. Log in to your [Fivetran account](https://fivetran.com/login).
2. Go to the **Destinations** page and click **Add destination**.
3. Enter a **Destination name** of your choice and then click **Add**.
4. Select **SurrealDB** as the destination type.
5. Put the `url` and `token` you found in the previous step
6. Click **Save & Test**.

   Fivetran [tests and validates](/docs/destinations/newdestination/setup-guide#setuptests) the SurrealDB connection. Upon successfully completing the setup tests, you can sync your data using Fivetran connectors to the SurrealDB destination.


### Setup tests

Fivetran performs the following SurrealDB connection tests:

- The Database Connection test checks if we can connect to your SurrealDB database using the provided URL and the token

  > NOTE: The test should complete in a few seconds if your Fivetran deployment can access the target SurrealDB instance.

---

## Related articles

[<i aria-hidden="true" class="material-icons">description</i> Destination Overview](/docs/destinations/surrealdb)

<b> </b>

[<i aria-hidden="true" class="material-icons">assignment</i> Release Notes](/docs/destinations/surrealdb/changelog)

<b> </b>

[<i aria-hidden="true" class="material-icons">settings</i> API Destination Configuration](/docs/rest-api/destinations/config#surrealdb)

<b> </b>

[<i aria-hidden="true" class="material-icons">home</i> Documentation Home](/docs/getting-started)
