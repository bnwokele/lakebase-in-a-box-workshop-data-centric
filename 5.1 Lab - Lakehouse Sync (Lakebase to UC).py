# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5.1: Lakehouse Sync — Lakebase to Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Direction 3 of 3: Outbound — Streaming OLTP into Delta for Analytics
# MAGIC
# MAGIC This is the third movement in the data-flow story. Together, the three labs map every direction
# MAGIC of data movement between Lakebase and the lakehouse:
# MAGIC
# MAGIC | Direction | Lab | Mechanism | Best for |
# MAGIC |---|---|---|---|
# MAGIC | UC → Lakebase | 3.1 | Synced Tables | Serving Lakehouse data to apps |
# MAGIC | Live read-through | 4.1 | UC foreign catalog (federation) | Ad-hoc joins, governed reads |
# MAGIC | **Lakebase → UC** | **5.1 (this lab)** | **Lakehouse Sync** | **High-throughput analytics on OLTP data** |
# MAGIC
# MAGIC In this lab you'll set up Lakehouse Sync so the live `orders`, `customers`, and `order_items`
# MAGIC tables in Lakebase are continuously mirrored as Delta tables in Unity Catalog. Once that's
# MAGIC running, BI dashboards, ML pipelines, and ad-hoc analytical queries can hit Delta — getting
# MAGIC full lakehouse performance — without putting any load on the OLTP database that powers the
# MAGIC storefront.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC 1. **Explain** what Lakehouse Sync is and how it complements Synced Tables (Lab 3.1) and
# MAGIC    federation (Lab 4.1)
# MAGIC 2. **Create** a Lakehouse Sync configuration that mirrors Lakebase tables to UC Delta
# MAGIC 3. **Trigger** the initial snapshot and verify Delta tables appear in UC
# MAGIC 4. **Demonstrate** end-to-end propagation by inserting a row in Lakebase and observing it in Delta
# MAGIC 5. **Run** an analytics query on the Delta-side data — "OLTP analytics without OLTP load"
# MAGIC
# MAGIC > **Docs**: [Lakehouse Sync](https://docs.databricks.com/aws/en/oltp/projects/lakehouse-sync)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Concept: Why Sync OLTP into Delta?
# MAGIC
# MAGIC Federation (Lab 4.1) is great for **live, low-volume** queries. But if a BI dashboard scans
# MAGIC every order from the last 30 days and you have a million orders, federation will:
# MAGIC
# MAGIC - Saturate the Lakebase compute (which is also serving the storefront)
# MAGIC - Run slowly because OLTP storage is row-oriented, not columnar
# MAGIC - Cost the same as serving live application traffic
# MAGIC
# MAGIC **Lakehouse Sync** addresses this by keeping a continuously-updated Delta replica of your
# MAGIC OLTP tables in Unity Catalog. Analytics queries hit Delta — columnar storage, photon
# MAGIC acceleration, and zero contention with the storefront.
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────┐                  ┌──────────────────────────────┐
# MAGIC │   Lakebase (production) │                  │    Unity Catalog (Delta)     │
# MAGIC │  ─────────────────────  │   Lakehouse Sync │  ──────────────────────────  │
# MAGIC │   ecommerce.orders       │ ─────────────▶ │   main.datacart_uc.orders     │
# MAGIC │   ecommerce.customers    │      CDC         │   main.datacart_uc.customers  │
# MAGIC │   ecommerce.order_items  │                  │   main.datacart_uc.order_items│
# MAGIC │                         │                  │                              │
# MAGIC │  Storefront (writes)    │                  │  BI / dashboards / ML (reads)│
# MAGIC └─────────────────────────┘                  └──────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC The OLTP side keeps the storefront happy. The Delta side absorbs heavy analytical scans.
# MAGIC Both stay in sync via Lakebase's managed CDC.

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

# Bundle-deployed Lakebase project
project_name = "datacart-data-centric"

# Where the synced Delta tables will land
UC_CATALOG = "main"
UC_SCHEMA = "datacart_uc"
TABLES_TO_SYNC = ["orders", "customers", "order_items"]

print(f"User:             {w.current_user.me().user_name}")
print(f"Lakebase project: {project_name}")
print(f"Sync target:      {UC_CATALOG}.{UC_SCHEMA}")
print(f"Tables:           {', '.join(TABLES_TO_SYNC)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Prepare the Target UC Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
print(f"✅ Schema {UC_CATALOG}.{UC_SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create the Lakehouse Sync Configuration
# MAGIC
# MAGIC Lakehouse Sync is configured at the project level. The cleanest way to set it up is via the
# MAGIC Databricks UI — that's what we'll walk through here. (You can also do this via the SDK /
# MAGIC REST API; see the docs link above.)
# MAGIC
# MAGIC ### UI walkthrough
# MAGIC
# MAGIC 1. Open **Catalog Explorer** in the sidebar.
# MAGIC 2. Navigate to your Lakebase project: **Lakebase Postgres** → `datacart-data-centric` → **production** branch.
# MAGIC 3. In the project page, click **Sync to Unity Catalog** (top-right of the Tables section).
# MAGIC 4. In the dialog:
# MAGIC    - **Target catalog:** `main`
# MAGIC    - **Target schema:** `datacart_uc`
# MAGIC    - **Source schema:** `ecommerce`
# MAGIC    - **Tables to sync:** check `orders`, `customers`, `order_items`
# MAGIC    - **Sync mode:** *Continuous* (or *Triggered* if you'd rather control when sync runs)
# MAGIC 5. Click **Create sync**. The pipeline provisions in ~1 minute and immediately runs an
# MAGIC    initial snapshot.
# MAGIC
# MAGIC > **Permissions:** the project owner (you) automatically has the rights to create the sync.
# MAGIC > In team setups, the project owner grants `CAN_USE` on the project to whoever is operating
# MAGIC > the sync pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify the Initial Snapshot Landed in Delta

# COMMAND ----------

import time

target_tables = [f"{UC_CATALOG}.{UC_SCHEMA}.{t}" for t in TABLES_TO_SYNC]

for fq in target_tables:
    for attempt in range(12):  # up to ~2 minutes
        try:
            count = spark.sql(f"SELECT COUNT(*) FROM {fq}").collect()[0][0]
            print(f"✅ {fq}: {count} rows")
            break
        except Exception as e:
            if attempt == 11:
                print(f"❌ {fq}: still not visible — check the sync pipeline status in the UI")
                print(f"   error: {e}")
            else:
                time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: End-to-End Propagation Demo
# MAGIC
# MAGIC We'll insert a new row directly into Lakebase, then wait for Lakehouse Sync to pick it up and
# MAGIC land it on the Delta side. This proves the loop is closed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Connect to Lakebase

# COMMAND ----------

# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

import psycopg2
import time

# Find the production endpoint
prod_branch = next(
    b for b in w.postgres.list_branches(parent=f"projects/{project_name}")
    if b.status and b.status.default
)
endpoint = next(iter(w.postgres.list_endpoints(parent=prod_branch.name)))
pg_host = endpoint.status.hosts.host

# Generate an OAuth token
cred = w.postgres.generate_database_credential(endpoint=endpoint.name)

conn = psycopg2.connect(
    host=pg_host,
    port=5432,
    database="databricks_postgres",
    user=w.current_user.me().user_name,
    password=cred.token,
    sslmode="require",
)
conn.autocommit = True
print(f"✅ Connected to Lakebase production via {pg_host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Insert a New Order

# COMMAND ----------

import uuid

probe_marker = f"sync-probe-{uuid.uuid4().hex[:8]}"

with conn.cursor() as cur:
    cur.execute("""
        INSERT INTO ecommerce.orders (customer_id, status, total)
        VALUES (1, %s, 19.99)
        RETURNING order_id, status, total;
    """, (probe_marker,))
    row = cur.fetchone()

print(f"✅ Inserted order_id={row[0]}, status={row[1]}, total={row[2]}")
print(f"   Marker (in status field): {probe_marker}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. Watch the Row Land in Delta

# COMMAND ----------

target_orders = f"{UC_CATALOG}.{UC_SCHEMA}.orders"

for attempt in range(30):  # up to ~5 minutes
    rows = spark.sql(
        f"SELECT order_id, status, total FROM {target_orders} WHERE status = '{probe_marker}'"
    ).collect()
    if rows:
        print(f"✅ Found probe order in {target_orders} after ~{attempt * 10}s:")
        for r in rows:
            print(f"   order_id={r['order_id']} status={r['status']} total={r['total']}")
        break
    print(f"   not yet visible (attempt {attempt + 1}/30)…")
    time.sleep(10)
else:
    print(f"❌ Probe order didn't appear in Delta after 5 minutes — check sync pipeline status.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: The Analytics Moment — OLTP Analytics Without OLTP Load
# MAGIC
# MAGIC Now the fun part. You can run heavy analytical aggregations against the Delta replica and pay
# MAGIC zero cost on the OLTP side. The storefront keeps serving customers; the analyst gets full
# MAGIC photon performance.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COUNT(*)               AS orders,
# MAGIC   ROUND(SUM(total), 2)   AS lifetime_revenue,
# MAGIC   ROUND(AVG(total), 2)   AS avg_order_value
# MAGIC FROM main.datacart_uc.orders
# MAGIC GROUP BY customer_id
# MAGIC ORDER BY lifetime_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC > **Cost test:** run an `EXPLAIN` on the same logical query against the foreign catalog
# MAGIC > (`lakebase_datacart.ecommerce.orders` from Lab 4.1). You'll see the federated plan pushes a
# MAGIC > full scan + aggregation down to Lakebase — fine for ad-hoc, expensive at scale. The Delta
# MAGIC > version above runs entirely on the SQL warehouse with photon. Same data, very different
# MAGIC > resource profile. **That tradeoff is the entire reason Lakehouse Sync exists.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: What Happens When Lakebase Schemas Change?
# MAGIC
# MAGIC In Labs 6.2 (Schema Migration) and 6.3 (Branch Reset) you'll add new columns to `customers`
# MAGIC and `orders` on the Lakebase side. Lakehouse Sync handles schema evolution:
# MAGIC
# MAGIC - **New columns** appear in the Delta tables on the next sync cycle.
# MAGIC - **Dropped columns** stop being written; existing Delta history is preserved.
# MAGIC - **Renamed columns** are treated as drop + add; rename through migration tooling explicitly
# MAGIC   to avoid that.
# MAGIC
# MAGIC In Lab 6.2, after applying the migration, come back and re-query `main.datacart_uc.customers`
# MAGIC — the new `loyalty_points` column will appear in Delta with no extra work on your side.
# MAGIC
# MAGIC In Lab 7.1 (PITR), if you DROP `orders` on Lakebase, the sync pipeline pauses and reports an
# MAGIC error. After PITR recovery, restart the pipeline if it gave up. The Delta side stays
# MAGIC consistent with the post-recovery Lakebase state.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Summary
# MAGIC
# MAGIC - You set up a Lakehouse Sync that mirrors three Lakebase tables to Delta in UC.
# MAGIC - You verified an end-to-end write path: storefront-style insert in Lakebase → Delta replica.
# MAGIC - You ran a customer-LTV aggregation against Delta — exactly the workload you don't want
# MAGIC   running directly on the OLTP database.
# MAGIC - You now have a complete picture of all three Lakebase ↔ Lakehouse data movements:
# MAGIC   inbound (Synced Tables, Lab 3.1), live (Federation, Lab 4.1), outbound (this lab).
# MAGIC
# MAGIC With those three primitives in your toolkit, the rest of the workshop (branching, schema
# MAGIC migration, PITR) is about safely *evolving* the OLTP side while these data flows continue
# MAGIC to operate. That's where we go next.

# COMMAND ----------

conn.close()

