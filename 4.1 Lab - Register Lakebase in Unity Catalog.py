# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4.1: Register Lakebase in Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Direction 2 of 3: Live Read-Through from UC into Lakebase
# MAGIC
# MAGIC In Lab 3.1 you pushed curated Lakehouse data **into** Lakebase via a Synced Table. This lab
# MAGIC is the second movement in the data-flow story: **register your Lakebase database as a foreign
# MAGIC catalog in Unity Catalog**, so analytics tools can query the live OLTP data — with no ETL,
# MAGIC no copy, no staleness — and join it against Delta tables in the lakehouse.
# MAGIC
# MAGIC | Direction | Lab | Mechanism |
# MAGIC |---|---|---|
# MAGIC | UC → Lakebase | 3.1 | Synced Tables |
# MAGIC | **Live read-through** | **4.1 (this lab)** | **Lakehouse Federation (foreign catalog)** |
# MAGIC | Lakebase → UC | 5.1 | Lakehouse Sync |
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC 1. **Explain** the difference between Synced Tables (materialized) and Lakehouse Federation (live)
# MAGIC 2. **Create** a Unity Catalog connection to a Lakebase Autoscaling project
# MAGIC 3. **Register** the Lakebase database as a Unity Catalog foreign catalog
# MAGIC 4. **Run** a federated join between live OLTP data and Delta analytics data — zero ETL
# MAGIC 5. **Reason** about when to use federation vs. Lakehouse Sync (covered in Lab 5.1)
# MAGIC
# MAGIC > **Docs**: [Register a Lakebase database in Unity Catalog](https://docs.databricks.com/aws/en/oltp/projects/register-uc) | [Lakehouse Federation](https://docs.databricks.com/aws/en/query-federation/)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Concept: Lakehouse Federation vs. Synced Tables
# MAGIC
# MAGIC Both let UC consumers see Lakebase data, but they have very different semantics:
# MAGIC
# MAGIC | Aspect | Synced Tables (Lab 3.1) | Foreign Catalog (this lab) |
# MAGIC |---|---|---|
# MAGIC | **Direction of data movement** | UC → Lakebase | Read-through; no movement |
# MAGIC | **Data freshness** | As fresh as the sync mode (snapshot / triggered / continuous) | Always live (queries hit Lakebase compute directly) |
# MAGIC | **Where the query runs** | Postgres compute on the Lakebase project | SQL warehouse pushes predicates down to Lakebase |
# MAGIC | **Best for** | Powering low-latency app reads from analytics-curated data | Ad-hoc analytical queries against live OLTP, joins with Delta |
# MAGIC | **Cost profile** | Sync compute + Postgres storage of the copy | Lakebase R/W compute usage on every query |
# MAGIC | **Write capability** | Read-only on the Lakebase side | Read-only on the UC side |
# MAGIC
# MAGIC **The data-engineer test:** if the question is *"can analysts run dashboards on live order data
# MAGIC without spinning up an ETL pipeline?"*, federation is the right tool. If the question is
# MAGIC *"can the storefront app serve a curated promo list with sub-10ms latency?"*, that's a synced
# MAGIC table.
# MAGIC
# MAGIC > **Heads up:** UC foreign catalogs registered from Lakebase are **read-only** through Unity
# MAGIC > Catalog. Writes still go through the Postgres protocol, the storefront app, or your sync
# MAGIC > pipelines. UC governs reads.

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Bundle-deployed Lakebase project (datacart-storefront/databricks.yml)
project_name = "datacart-data-centric"

# Unity Catalog targets — adjust to your workspace
UC_CATALOG = "main"           # the catalog where we'll create the Delta marketing_campaigns table
UC_SCHEMA = "datacart_demo"   # schema within UC_CATALOG
FOREIGN_CATALOG = "lakebase_datacart"   # the foreign catalog name we'll register
CONNECTION_NAME = "lakebase_datacart_conn"

print(f"User:             {w.current_user.me().user_name}")
print(f"Lakebase project: {project_name}")
print(f"Foreign catalog:  {FOREIGN_CATALOG}")
print(f"UC schema:        {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Find the Lakebase Hostname
# MAGIC
# MAGIC The UC connection needs the project's read-write hostname. Pull it from the SDK so you don't
# MAGIC have to copy-paste from the UI.

# COMMAND ----------

prod_branch = next(
    b for b in w.postgres.list_branches(parent=f"projects/{project_name}")
    if b.status and b.status.default
)
endpoint = next(iter(w.postgres.list_endpoints(parent=prod_branch.name)))
pg_host = endpoint.status.hosts.host
pg_port = 5432
pg_database = "databricks_postgres"

print(f"Lakebase host:     {pg_host}")
print(f"Lakebase port:     {pg_port}")
print(f"Lakebase database: {pg_database}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create the UC Connection (UI)
# MAGIC
# MAGIC Run the next cell in a SQL warehouse to create a Lakebase Postgres connection that uses your
# MAGIC own Databricks identity for authentication (OAuth). Substitute `pg_host` from Step 2 if the
# MAGIC bundled `${pg_host}` placeholder is not resolved by your warehouse.
# MAGIC
# MAGIC > **Auth pattern:** the connection uses **OAuth user-to-machine** so each query carries the
# MAGIC > caller's identity into Lakebase. Postgres role grants you set up in Lab 2.1 apply.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NOTE: Replace ${pg_host} below with the host printed in Step 2 if your warehouse does
# MAGIC -- not interpolate notebook variables.
# MAGIC CREATE CONNECTION IF NOT EXISTS lakebase_datacart_conn
# MAGIC TYPE postgresql
# MAGIC OPTIONS (
# MAGIC   host     '${pg_host}',
# MAGIC   port     '5432',
# MAGIC   database 'databricks_postgres',
# MAGIC   auth_type 'OAUTH_USER_TO_MACHINE'
# MAGIC )
# MAGIC COMMENT 'Lakebase Autoscaling project: datacart-data-centric (created in Lab 4.1)';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Register the Foreign Catalog
# MAGIC
# MAGIC One foreign catalog per Lakebase database. Once registered, it shows up in Catalog Explorer
# MAGIC like any other UC catalog and is queryable from any SQL warehouse / serverless SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS lakebase_datacart
# MAGIC USING CONNECTION lakebase_datacart_conn
# MAGIC OPTIONS (database 'databricks_postgres')
# MAGIC COMMENT 'Live foreign catalog into the datacart-data-centric Lakebase project';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Smoke-Test — Live Query Against Lakebase
# MAGIC
# MAGIC You should see the same `orders` rows you'd see if you logged into the Postgres instance
# MAGIC directly. The query is a real-time read — no copy, no replication lag.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, status, total
# MAGIC FROM lakebase_datacart.ecommerce.orders
# MAGIC ORDER BY order_id
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: The Value Moment — Federated Join with Delta Marketing Data
# MAGIC
# MAGIC This is the scenario data engineers care about. We have:
# MAGIC - **Live OLTP orders** in Lakebase (federated as `lakebase_datacart.ecommerce.orders`)
# MAGIC - **Curated marketing campaigns** in Delta (we'll create this in a moment)
# MAGIC
# MAGIC Without federation you'd have to ETL one into the other before joining. With federation, the
# MAGIC SQL warehouse pushes the predicate down to Lakebase, pulls back only the matching rows, and
# MAGIC joins them against Delta in-place.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Stage a tiny Delta table representing marketing-team-curated campaigns.
# MAGIC CREATE SCHEMA IF NOT EXISTS main.datacart_demo;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE main.datacart_demo.marketing_campaigns (
# MAGIC   campaign        STRING,
# MAGIC   utm_source      STRING,
# MAGIC   start_date      DATE,
# MAGIC   end_date        DATE,
# MAGIC   target_segment  STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO main.datacart_demo.marketing_campaigns VALUES
# MAGIC   ('Spring Kickoff',     'spring_email',  DATE '2026-03-01', DATE '2026-04-01', 'returning'),
# MAGIC   ('Loyalty Push',       'loyalty_push',  DATE '2026-04-01', DATE '2026-05-15', 'loyalty'),
# MAGIC   ('Cross-Channel Demo', 'paid_social',   DATE '2026-04-15', DATE '2026-06-01', 'new');

# COMMAND ----------

# MAGIC %md
# MAGIC ### The federated query
# MAGIC
# MAGIC > **Note:** The orders table created in Lab 1.1 doesn't have a `utm_source` column by default.
# MAGIC > For this demo we join on `customer_id` modulo the campaign array length so every order maps
# MAGIC > to a campaign. In a real setting, an `orders.utm_source` column populated by the storefront
# MAGIC > would replace this. The point is the *shape* of the query — federated join in one statement.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH live_orders AS (
# MAGIC   SELECT
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     total,
# MAGIC     status
# MAGIC   FROM lakebase_datacart.ecommerce.orders
# MAGIC ),
# MAGIC campaigns_indexed AS (
# MAGIC   SELECT campaign, utm_source, ROW_NUMBER() OVER (ORDER BY campaign) - 1 AS idx,
# MAGIC          (SELECT COUNT(*) FROM main.datacart_demo.marketing_campaigns) AS n
# MAGIC   FROM main.datacart_demo.marketing_campaigns
# MAGIC )
# MAGIC SELECT
# MAGIC   c.campaign,
# MAGIC   c.utm_source,
# MAGIC   COUNT(o.order_id)            AS attributed_orders,
# MAGIC   ROUND(SUM(o.total), 2)       AS attributed_revenue
# MAGIC FROM live_orders o
# MAGIC JOIN campaigns_indexed c
# MAGIC   ON (o.customer_id % c.n) = c.idx
# MAGIC GROUP BY c.campaign, c.utm_source
# MAGIC ORDER BY attributed_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **What just happened:**
# MAGIC - The SQL warehouse parsed the join.
# MAGIC - It pushed a `SELECT order_id, customer_id, total, status FROM ecommerce.orders` down to Lakebase.
# MAGIC - Lakebase returned the live OLTP rows.
# MAGIC - The warehouse joined them against the Delta `marketing_campaigns` table in-place.
# MAGIC
# MAGIC No ETL. No staleness. No second copy of the data. The lakehouse and the OLTP database
# MAGIC are queryable as one logical surface.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 7: Federation vs. Lakehouse Sync — Decision Notes
# MAGIC
# MAGIC | Use federation (this lab) when… | Use Lakehouse Sync (Lab 5.1) when… |
# MAGIC |---|---|
# MAGIC | The query is ad-hoc or low-frequency | The query runs many times per minute on the same data |
# MAGIC | The data must be live to the millisecond | Hourly or near-real-time freshness is acceptable |
# MAGIC | You need governance (UC tags, lineage) on read | You need to run heavy aggregations without OLTP load |
# MAGIC | The join touches small slices of OLTP | The query scans large fractions of OLTP tables |
# MAGIC | You want zero pipeline overhead | You can pay for a sync pipeline to amortize cost |
# MAGIC
# MAGIC In production, most data-centric teams use **both**: federation for live spot-checks /
# MAGIC governed read APIs, and Lakehouse Sync for high-throughput analytical workloads. The next
# MAGIC lab covers the latter.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - You created a UC connection that authenticates with **your own Databricks identity** —
# MAGIC   no shared secrets, no static passwords.
# MAGIC - You registered Lakebase as a UC foreign catalog so any SQL warehouse / serverless SQL /
# MAGIC   notebook can query OLTP data with the same identity model as Delta.
# MAGIC - You ran a federated join: live OLTP × Delta marketing data, in one statement, with no ETL.
# MAGIC - You now have a working mental model for **when to federate vs. when to sync** — the next
# MAGIC   lab (5.1 Lakehouse Sync) is where you'll see the analytics-throughput end of that decision.

