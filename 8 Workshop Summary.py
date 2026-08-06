# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](./Includes/images/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Workshop Summary & Recap
# MAGIC
# MAGIC Congratulations — you've completed the **Lakebase Workshop**. You stepped into the role of a database engineer at **DataCart**, a fast-growing e-commerce platform preparing for its "Spring Sale" launch. DataCart started out overprovisioning an expensive database for spiky traffic, refreshing a single shared dev database every weekend to keep it vaguely in sync with production, hand-rolling brittle ETL pipelines in both directions, and exposed to hours-long outages when something broke. Over these labs you replaced all of that with Lakebase: paying only for the compute you use, giving every developer an isolated branch, syncing data both ways with zero ETL, and recovering from a production disaster in seconds.
# MAGIC
# MAGIC This notebook recaps what you built, the order it happened in, and the Lakebase concepts behind each step. Use it as a quick reference when you apply these patterns to your own workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The DataCart story, end to end
# MAGIC
# MAGIC | # | Lab | What you accomplished |
# MAGIC |---|-----|-----------------------|
# MAGIC | **1** | Discover and Seed the Lakebase Project | Discovered a pre-provisioned Lakebase Autoscaling project, connected over OAuth, and seeded the `ecommerce` schema |
# MAGIC | **2** | Roles, Permissions, and Connecting the Storefront | Granted the storefront's service principal the database access it needs and brought the storefront online |
# MAGIC | **3** | Reverse ETL with Synced Tables (UC → Lakebase) | Pushed Spring Sale promotions from a Delta table into Lakebase so the storefront shows sale badges and discounts |
# MAGIC | **4** | Lakebase CDF (Lakebase → UC) | Continuously mirrored live OLTP tables to Delta in Unity Catalog for heavy analytics — without loading production |
# MAGIC | **5** | Parallel Development with Branching | Created isolated, zero-copy branches so three developers could evolve the schema in parallel |
# MAGIC | **6** | Schema Changes: Feature Branch to Production | Promoted validated changes from a feature branch to production using idempotent Migration Replay |
# MAGIC | **7** | Point-in-Time Recovery (PITR) & Snapshots | Simulated a "Code Red" dropped-table disaster and recovered production with zero data loss |
# MAGIC
# MAGIC > A companion setup notebook — **Create Lakebase Project & App (using SDK)** — provisions the Lakebase project and the DataCart storefront app if you need to (re)create them via the SDK instead of the bundle deploy path.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 1 — Discover and Seed the Lakebase Project
# MAGIC
# MAGIC **Objective:** Take a hands-on tour of a pre-provisioned Lakebase Autoscaling project by discovering it, connecting via OAuth, seeding an e-commerce schema, and exploring Postgres system metadata.
# MAGIC
# MAGIC **What you did**
# MAGIC - Discovered your Lakebase Autoscaling project via the SDK and located the default `production` branch and compute endpoint
# MAGIC - Connected to the Postgres database using short-lived **OAuth token** authentication (no passwords)
# MAGIC - Created and populated 5 e-commerce tables with native PL/pgSQL (SERIAL keys, foreign keys, CHECK/UNIQUE constraints, cascading deletes)
# MAGIC - Explored PostgreSQL system metadata via `pg_catalog`, `information_schema`, and `pg_stat_statements`
# MAGIC
# MAGIC **Key concepts:** Lakebase Autoscaling vs. Provisioned · OAuth token authentication · autoscaling compute & scale-to-zero · instant branching · `project_id` vs. `display_name` · PostgreSQL 17 system catalogs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 2 — Roles, Permissions, and Connecting the Storefront
# MAGIC
# MAGIC **Objective:** Understand Lakebase's two permission layers (workspace and database), then grant the DataCart Storefront app's service principal the Postgres access it needs to serve live data.
# MAGIC
# MAGIC **What you did**
# MAGIC - Identified the storefront app's service principal and connected to the production branch as project owner
# MAGIC - Granted the SP `CAN USE` on the Lakebase project and created its OAuth Postgres role via `databricks_create_role()`
# MAGIC - Granted schema/table/sequence privileges (plus `ALTER DEFAULT PRIVILEGES` for future tables) on the `ecommerce` schema
# MAGIC - Audited the grants with `pg_roles`, `information_schema`, and `has_table_privilege()`, then confirmed the storefront loads
# MAGIC
# MAGIC **Key concepts:** workspace vs. database permission layers · project ACLs (CAN CREATE / CAN USE / CAN MANAGE) · OAuth Postgres roles · `databricks_auth` extension · GRANT hierarchy (database → schema → tables/sequences) · `ALTER DEFAULT PRIVILEGES` · service principal authentication

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 3 — Reverse ETL with Synced Tables (UC → Lakebase)
# MAGIC
# MAGIC **Objective:** Move curated analytics data from a Unity Catalog Delta table into Lakebase Postgres via managed synced tables so the live storefront serves Spring Sale promotions with sub-10ms latency.
# MAGIC
# MAGIC **What you did**
# MAGIC - Created a Change-Data-Feed–enabled `promotions` Delta table in Unity Catalog and seeded Spring Sale data
# MAGIC - Created a Snapshot-mode **synced table** (UI and Terraform/IaC alternatives shown) into the production Lakebase branch
# MAGIC - Re-granted table/sequence permissions to the storefront service principal and verified promotions went live
# MAGIC - Updated the Delta table with new flash-sale promos, triggered a re-sync, and confirmed the storefront reflected changes with **zero application code changes**
# MAGIC
# MAGIC **Key concepts:** Reverse ETL · Synced Tables · sync modes (Snapshot / Triggered / Continuous) · Change Data Feed · UC-to-Postgres type mapping · Unity Catalog governance · low-latency OLTP serving

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 4 — Lakebase CDF (Lakebase → UC)
# MAGIC
# MAGIC **Objective:** Set up Lakebase CDF to continuously mirror live Lakebase OLTP tables into Unity Catalog Delta tables for high-throughput analytics — without loading the production database.
# MAGIC
# MAGIC **What you did**
# MAGIC - Set `REPLICA IDENTITY FULL` on source Lakebase tables so logical replication captures UPDATEs and DELETEs
# MAGIC - Created a Lakebase CDF configuration targeting a UC catalog/schema
# MAGIC - Triggered the initial snapshot and verified the Delta tables landed in Unity Catalog
# MAGIC - Ran analytics queries against the Delta replica to demonstrate "OLTP analytics without OLTP load"
# MAGIC
# MAGIC **Key concepts:** Lakebase CDF · CDC / Postgres logical replication · `REPLICA IDENTITY FULL` · OLTP-to-Delta mirroring · schema evolution · Unity Catalog Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 5 — Parallel Development with Branching
# MAGIC
# MAGIC **Objective:** Learn how Lakebase Autoscaling database branching lets multiple developers make isolated, simultaneous schema changes without impacting production or each other.
# MAGIC
# MAGIC **What you did**
# MAGIC - Created zero-copy, TTL-expiring branches from the production branch via the Databricks SDK
# MAGIC - Simulated three developers working in parallel (loyalty features + reviews, multi-currency FK migration, performance indexes), each on their own branch
# MAGIC - Ran isolated DDL/DML migrations per branch and seeded branch-only data
# MAGIC - Verified the production branch remained untouched, proving schema isolation
# MAGIC
# MAGIC **Key concepts:** database branching · copy-on-write storage · zero-copy snapshots · expiring branches (TTL) · schema isolation · per-developer branch strategy · branch compute endpoints

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 6 — Schema Changes: Feature Branch to Production
# MAGIC
# MAGIC **Objective:** Promote validated schema changes (the `loyalty_points` column, `loyalty_members` and `reviews` tables) from a feature branch to production using the idempotent Migration Replay pattern.
# MAGIC
# MAGIC **What you did**
# MAGIC - Used **Schema Diff** to compare the feature branch against production before promoting
# MAGIC - Confirmed production was untouched, then replayed the same idempotent DDL on production
# MAGIC - Seeded product reviews data and watched the storefront pick up the changes live
# MAGIC - Cleaned up the feature branch (delete or TTL expiry)
# MAGIC
# MAGIC **Key concepts:** Schema Diff · Migration Replay (idempotent DDL) · Branch Reset · feature-branch-to-production promotion · branch lifecycle

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 7 — Point-in-Time Recovery (PITR) & Snapshots
# MAGIC
# MAGIC **Objective:** See how Lakebase PITR and Snapshots protect data by simulating a production disaster (a dropped table) and recovering from it hands-on.
# MAGIC
# MAGIC **What you did**
# MAGIC - Recorded a pre-disaster timestamp, then simulated a "Code Red" disaster with `DROP TABLE orders CASCADE` on production
# MAGIC - Created a PITR recovery branch from the pre-disaster point in time and verified the data was intact
# MAGIC - Restored production by recreating the table and copying data back from the PITR branch
# MAGIC - Confirmed the storefront detected the restored data and came back online
# MAGIC
# MAGIC **Key concepts:** Point-in-Time Recovery (PITR) · restore window · Snapshots (manual & scheduled) · root branches · non-destructive recovery · branch TTL

# COMMAND ----------

# MAGIC %md
# MAGIC ## The big picture: what Lakebase gave you
# MAGIC
# MAGIC | Capability | The pain it removed for DataCart |
# MAGIC |---|---|
# MAGIC | **Serverless autoscaling & scale-to-zero** | No more overprovisioning for peak traffic — compute scales to demand and to **zero** when idle, so DataCart pays only for what it uses |
# MAGIC | **Decoupled compute & storage** | The foundation that makes autoscaling and zero-copy branches possible — data lives in cheap object storage in open formats, independent of compute |
# MAGIC | **Zero-copy branching** | Ended the single shared dev database that drifted from prod and the weekend refreshes — every developer gets an isolated, production-like database in seconds |
# MAGIC | **No-ETL bidirectional sync** | Retired the brittle hand-built pipelines — Reverse ETL (UC → Lakebase) serves analytics data to the app and Lakebase CDF (Lakebase → UC) feeds analytics from OLTP, both fully managed |
# MAGIC | **Point-in-Time Recovery** | Turned an hours-long, revenue-losing outage into a seconds-long recovery — no nightly-backup hunt |
# MAGIC | **Unified governance** | Service principals, OAuth roles, and Unity Catalog kept access controlled across branches and the lakehouse |
# MAGIC
# MAGIC ### Where to go next
# MAGIC - Apply the **branch → migrate → diff → replay** pattern to your own schema changes
# MAGIC - Use **Synced Tables** and **Lakebase CDF** to connect your operational apps and analytics without bespoke ETL
# MAGIC - Build **scale-to-zero developer sandboxes** and AI-agent environments on branches
# MAGIC - Make **PITR and Snapshots** part of your operational runbook
# MAGIC
# MAGIC Thanks for participating in the Lakebase Workshop!

