# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 7: Point-in-Time Recovery (PITR) & Snapshots
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC This lab covers two key data protection features of Lakebase: **Point-in-Time Recovery (PITR)**
# MAGIC and **Snapshots**. You'll learn the concepts behind each, then apply PITR hands-on by simulating
# MAGIC a production disaster and recovering from it.
# MAGIC
# MAGIC ## Why this lab matters
# MAGIC
# MAGIC > **📍 DataCart's journey** — DataCart once had a DevOps engineer drop a production table by mistake, taking key storefront functionality down for hours and costing real revenue while the team hunted for a backup. With Lakebase **PITR**, that same recovery takes seconds.
# MAGIC
# MAGIC Now that you've evolved the OLTP schema in production (Lab 6), the storefront is running its
# MAGIC full feature set. But a single bad migration or accidental `DROP TABLE` can take production
# MAGIC down in an instant. PITR is the safety net: it lets you rewind a branch to **any exact moment**
# MAGIC within the restore window and recover with zero data loss — no nightly-backup hunt, no hours of
# MAGIC downtime. We'll prove it by breaking production on purpose and bringing it back.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC 1. **Explain** what PITR is and how the restore window works
# MAGIC 2. **Explain** what Snapshots are and when to use them vs. PITR
# MAGIC 3. **Create** a PITR recovery branch from a specific point in time
# MAGIC 4. **Restore** production data after an accidental destructive operation
# MAGIC
# MAGIC > **Docs**: [Point-in-time restore](https://docs.databricks.com/aws/en/oltp/projects/point-in-time-restore) | [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Point-in-Time Recovery (PITR)
# MAGIC
# MAGIC PITR lets you restore a branch to **any exact moment** within a configurable window. It is powered by the same transaction log that Lakebase maintains for all root branches — no extra setup required.
# MAGIC
# MAGIC ### What is a Restore Window?
# MAGIC
# MAGIC The **restore window** is how far back in time you can recover. It is configurable from **0 to 30 days** and applies uniformly across *all* branches in the project.
# MAGIC
# MAGIC | Setting | Effect |
# MAGIC |---------|--------|
# MAGIC | Longer window (e.g. 30 days) | More recovery flexibility, but higher storage cost |
# MAGIC | Shorter window (e.g. 1 day) | Lower storage cost, but limited recovery range |
# MAGIC | 0 days | PITR is effectively disabled |
# MAGIC
# MAGIC > The restore window is a **project-level setting** — you cannot set different windows per branch.
# MAGIC
# MAGIC ### How to Perform a Restore
# MAGIC
# MAGIC PITR can be performed through the **Lakebase UI** or the **SDK** (as we'll do in this lab):
# MAGIC
# MAGIC 1. Open your project → **Backup & Restore**
# MAGIC 2. Select your source branch
# MAGIC 3. Use the date/time picker to choose your restore point
# MAGIC 4. Click **Restore to point in time**
# MAGIC
# MAGIC <img src="Includes/images/pitr/backup_restore_1.png" alt="Backup & Restore UI" width="800">
# MAGIC <img src="Includes/images/pitr/backup_restore_2.png" alt="Select branch and time" width="800">
# MAGIC <img src="Includes/images/pitr/backup_restore_3.png" alt="Confirm restore" width="800">
# MAGIC
# MAGIC ### What Happens After a Restore?
# MAGIC
# MAGIC A restore **never modifies your existing branch**. Instead:
# MAGIC
# MAGIC | Outcome | Detail |
# MAGIC |---------|--------|
# MAGIC | **New root branch created** | Contains the full database state from the specified point in time |
# MAGIC | **Original branch unchanged** | Your production branch keeps running without interruption |
# MAGIC | **Existing connections unaffected** | Apps connected to the original branch see no disruption |
# MAGIC | **Manual cutover required** | To use the restored data, update your app's connection string to the new branch |
# MAGIC
# MAGIC > Projects support a maximum of **3 root branches**. If you're at the limit, delete one before restoring.
# MAGIC
# MAGIC > A restore recovers **all databases** within a branch — you cannot restore a single database in isolation.
# MAGIC
# MAGIC ### When to Use PITR
# MAGIC
# MAGIC PITR is optimized for **unexpected, unplanned events** where you need to recover to a precise moment:
# MAGIC
# MAGIC | Scenario | Example |
# MAGIC |----------|---------|
# MAGIC | Accidental data deletion | `DELETE FROM orders` without a `WHERE` clause |
# MAGIC | Destructive schema changes | `DROP TABLE inventory_main` run on the wrong environment |
# MAGIC | Application bug corruption | A code deploy that wrote bad data for 10 minutes |
# MAGIC | ~~Planned pre-change backup~~ | Use a **Snapshot** instead — it's more explicit and cheaper |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Snapshots
# MAGIC
# MAGIC A snapshot is an **explicit, named point-in-time capture** of a root branch. Unlike the continuous PITR transaction log, snapshots are discrete restore points you create on demand or on a schedule.
# MAGIC
# MAGIC ### Key Properties
# MAGIC
# MAGIC | Property | Detail |
# MAGIC |----------|--------|
# MAGIC | **Instant creation** | Snapshots are created immediately with minimal performance impact |
# MAGIC | **Root branches only** | Snapshots can only be taken on root (production-level) branches |
# MAGIC | **Manual limit** | Up to **10 manual snapshots** per project |
# MAGIC | **Scheduled snapshots** | Do not count toward the 10-snapshot limit |
# MAGIC | **Deletion is permanent** | Deleted snapshots cannot be recovered |
# MAGIC
# MAGIC ### Snapshot Schedules
# MAGIC
# MAGIC Automated snapshots run at regular intervals so you always have a recent restore point:
# MAGIC
# MAGIC 1. Open your project → **Backup & Restore**
# MAGIC 2. Click **Edit schedule**
# MAGIC 3. Choose frequency: **Daily** | **Weekly** | **Monthly**
# MAGIC 4. Set your retention period → **Update Schedule**
# MAGIC
# MAGIC > When a scheduled snapshot's retention period expires, it is **automatically deleted**. Manual snapshots persist until you explicitly delete them.
# MAGIC
# MAGIC ### Restoring from a Snapshot
# MAGIC
# MAGIC The same non-destructive restore model applies as PITR — a new root branch is created, named `branch_from_snapshot_<timestamp>`. Your original branch continues operating normally.
# MAGIC
# MAGIC <img src="Includes/images/pitr/restore_1.png" alt="Restore from Snapshot" width="800">
# MAGIC
# MAGIC ### When to Use Snapshots
# MAGIC
# MAGIC Snapshots are optimized for **planned, proactive backups**:
# MAGIC
# MAGIC | Scenario | Example |
# MAGIC |----------|---------|
# MAGIC | Before risky schema migrations | `ALTER TABLE` that drops columns or changes types |
# MAGIC | Before a major deployment | Spring Sale go-live cutover |
# MAGIC | Regular scheduled backups | Daily snapshot at 02:00 UTC as a safety net |
# MAGIC | Compliance checkpoints | End-of-month data freeze for audit purposes |
# MAGIC | ~~Recovering from an unknown moment~~ | Use **PITR** — you need granularity beyond snapshot frequency |

# COMMAND ----------

# MAGIC %md
# MAGIC ## PITR vs. Snapshots — Quick Reference
# MAGIC
# MAGIC | | **PITR** | **Snapshots** |
# MAGIC |---|---|---|
# MAGIC | **Best for** | Unexpected events (accidents, bugs) | Planned events (deployments, migrations) |
# MAGIC | **Granularity** | Any second within the restore window | Discrete named points |
# MAGIC | **Window / Limit** | 0-30 days (project-wide) | 10 manual + unlimited scheduled |
# MAGIC | **Storage cost** | Increases with longer window | Per-snapshot overhead |
# MAGIC | **Setup required** | None — always on for root branches | Scheduled or manual creation |
# MAGIC | **Restore target** | New root branch | New root branch |
# MAGIC | **Original branch** | Unchanged | Unchanged |
# MAGIC | **Root branch limit** | Max 3 per project | Max 3 per project |
# MAGIC
# MAGIC > **Rule of thumb:** Use Snapshots *before* you make a change. Use PITR *after* something goes wrong.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Hands-On: Disaster Recovery with PITR
# MAGIC
# MAGIC **The Challenge:**
# MAGIC A DevOps engineer accidentally executes `DROP TABLE orders` instead of a temp staging table.
# MAGIC The production app starts throwing 500 errors. Customer orders are gone. Revenue reporting is broken.
# MAGIC
# MAGIC **The Lakebase Solution: Point-in-Time Recovery**
# MAGIC With Lakebase PITR, the team creates a branch from **1 minute before the disaster**, verifies the
# MAGIC data is intact, and restores the orders table — all without downtime or backup tapes.
# MAGIC
# MAGIC ```
# MAGIC Timeline:
# MAGIC ──────────────────────────────────────────────────────────────────
# MAGIC   T-1min          T=0 (disaster)        T+5min (recovery)
# MAGIC   ───┬──────────────┬──────────────────────┬───
# MAGIC      │              │                      │
# MAGIC      │         DROP TABLE orders      CREATE PITR branch
# MAGIC      │                                from T-1min
# MAGIC      │                                     │
# MAGIC      └─── PITR branch has orders! ─────────┘
# MAGIC                                            │
# MAGIC                                     Copy data back
# MAGIC                                     to production
# MAGIC ```
# MAGIC
# MAGIC > **Key Insight:** Lakebase retains a full history of changes (configurable retention, default 24h).
# MAGIC > You can create a branch from **any point in time** within that window.

# COMMAND ----------

# MAGIC %md
# MAGIC ### How we'll do it — the SDK
# MAGIC
# MAGIC In the real world, point-in-time recovery is an **API operation** — you drive it from the SDK (or
# MAGIC the Backup & Restore UI it's built on), not by hand-copying rows in a SQL editor. So we'll run the
# MAGIC whole disaster→recovery sequence programmatically with `psycopg2` and the Databricks SDK.
# MAGIC
# MAGIC The setup cells below install dependencies and define the branch helpers. Then we'll walk the
# MAGIC incident step by step — health check → disaster → create a recovery branch → copy the data back —
# MAGIC pausing at three **Storefront Checkpoints** so you can open the DataCart app and watch it break and
# MAGIC recover in real time.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 0: Install Dependencies & Configure Helpers

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import time
import psycopg2

w = WorkspaceClient()

# Bundle-deployed Lakebase project (datacart-storefront/databricks.yml)
# Project name is auto-derived per user from ${workspace.current_user.id}
project_name = f"lakebase-workshop-{w.current_user.me().id}"
db_user = w.current_user.me().user_name

# List branches — the default 'production' branch should exist
branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))

print(f"📋 Branches in '{project_name}':")
for b in branches:
    branch_id = b.name.split("/branches/")[-1]
    is_default = "⭐ default" if b.status and b.status.default else ""
    print(f"   • {branch_id} {is_default}")

# Get the production branch (the default one, or fallback to the first)
prod_branch = next(
    (b for b in branches if b.status and b.status.default),
    branches[0]
)
prod_branch_name = prod_branch.name
print(f"\n✅ Production branch: {prod_branch_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: Connect to Any Branch

# COMMAND ----------

# Fixed configuration
db_schema = "ecommerce"
min_cu = 0.5
max_cu = 4.0
suspend_timeout_seconds = 1800

def connect_to_branch(branch_id, wait_seconds=300):
    """
    Connect to a Lakebase branch endpoint.
    Automatically creates a compute endpoint if none exists.
    """
    from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, Duration as Dur

    branch_full = f"projects/{project_name}/branches/{branch_id}"

    # Check if an endpoint already exists
    endpoints = list(w.postgres.list_endpoints(parent=branch_full))

    if not endpoints:
        ep_id = f"ep-{branch_id}"
        print(f"🔄 Creating compute endpoint for branch '{branch_id}'...")
        w.postgres.create_endpoint(
            parent=branch_full,
            endpoint=Endpoint(spec=EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=min_cu,
                autoscaling_limit_max_cu=max_cu,
                suspend_timeout_duration=Dur(seconds=suspend_timeout_seconds)
            )),
            endpoint_id=ep_id
        ).wait()
        print(f"   ✅ Compute endpoint created!")
        endpoints = list(w.postgres.list_endpoints(parent=branch_full))

    ep = endpoints[0]
    if not ep.status or not ep.status.hosts or not ep.status.hosts.host:
        print(f"⏳ Waiting for endpoint to become ready...")
        for i in range(wait_seconds // 10):
            time.sleep(10)
            endpoints = list(w.postgres.list_endpoints(parent=branch_full))
            ep = endpoints[0]
            if ep.status and ep.status.hosts and ep.status.hosts.host:
                break
            print(f"   Still waiting... ({(i+1)*10}s)")

    if not ep.status or not ep.status.hosts or not ep.status.hosts.host:
        raise Exception(f"Endpoint not ready for branch '{branch_id}' after {wait_seconds}s")

    host = ep.status.hosts.host

    cred = w.postgres.generate_database_credential(endpoint=ep.name)
    branch_conn = psycopg2.connect(
        host=host,
        port=5432,
        dbname="databricks_postgres",
        user=db_user,
        password=cred.token,
        sslmode="require"
    )
    branch_conn.autocommit = True

    print(f"✅ Connected to branch '{branch_id}'")
    print(f"   Host: {host}")
    return branch_conn, host, ep.name

def delete_branch_safe(branch_id, max_retries=6, wait_between=30):
    """Delete a branch, retrying if the endpoint is still reconciling."""
    branch_full = f"projects/{project_name}/branches/{branch_id}"

    for attempt in range(max_retries):
        try:
            w.postgres.delete_branch(name=branch_full).wait()
            print(f"🗑️ Branch '{branch_id}' deleted.")
            return
        except Exception as e:
            if "reconciliation" in str(e).lower() and attempt < max_retries - 1:
                print(f"   ⏳ Endpoint still reconciling, retrying in {wait_between}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_between)
            else:
                raise

def print_table(cols, rows, max_rows=30):
    if not cols:
        print("(no results)")
        return
    widths = [max(len(str(c)), max((len(str(r[i])) for r in rows), default=0)) for i, c in enumerate(cols)]
    sep    = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    print(sep)
    print("|" + "|".join(f" {c:<{widths[i]}} " for i, c in enumerate(cols)) + "|")
    print(sep)
    for row in rows[:max_rows]:
        print("|" + "|".join(f" {str(v):<{widths[i]}} " for i, v in enumerate(row)) + "|")
    print(sep)

print("🔧 Helpers defined: connect_to_branch(), delete_branch_safe(), print_table()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verify production is healthy
# MAGIC
# MAGIC Connect to the **`production`** branch and confirm the data is intact. Note the order count and
# MAGIC revenue so you can compare after recovery.

# COMMAND ----------

# 1) Connect to production and confirm baseline
conn_prod, _, _ = connect_to_branch('production')
print("📊 Production baseline:")
with conn_prod.cursor() as cur:
    for table in ['customers', 'products', 'orders']:
        try:
            cur.execute(f"SELECT count(*) FROM {db_schema}.{table}")
            print(f"   ✅ {table}: {cur.fetchone()[0]} rows")
        except Exception as e:
            print(f"   🔴 {table}: {str(e).splitlines()[0]}")

    cur.execute(f"SELECT COALESCE(SUM(total), 0) FROM {db_schema}.orders")
    print(f"   💰 Total revenue: ${cur.fetchone()[0]:,.2f}")

print("\n✅ Production is healthy. All tables present and populated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storefront Checkpoint 1: Everything is Healthy
# MAGIC
# MAGIC Open the **DataCart Storefront** now and note the full feature set:
# MAGIC - Products display with star ratings, stock badges, and "Earn X pts" labels
# MAGIC - Best Sellers and Top Rated sections work on the homepage
# MAGIC - Orders page shows full order history
# MAGIC - Cart and checkout function normally
# MAGIC
# MAGIC > Take a mental snapshot. In a moment, things will break.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Record the "before" timestamp, then simulate the disaster
# MAGIC
# MAGIC First we capture the current time **before** the disaster — this is the moment we'll rewind to.
# MAGIC Then a DevOps engineer means to drop a staging table and drops the real `orders` table instead.
# MAGIC
# MAGIC > In a real incident you'd get the recovery timestamp from your monitoring/alerting — the moment
# MAGIC > just before things went wrong.

# COMMAND ----------

import datetime

# 2) Record the "before" timestamp
with conn_prod.cursor() as cur:
    cur.execute("SELECT NOW()")
    before_timestamp = cur.fetchone()[0]
before_epoch = int(before_timestamp.timestamp())
print(f"⏱️  Recovery point: {before_timestamp} ({before_epoch} epoch seconds)")

# Give the transaction log a moment so the restore point is clearly before the drop
time.sleep(5)

# 3) Disaster: drop the orders table
with conn_prod.cursor() as cur:
    cur.execute(f"DROP TABLE IF EXISTS {db_schema}.orders CASCADE")
print("\n💥 DROPPED ecommerce.orders — production is now broken.")

# 4) Confirm the damage
with conn_prod.cursor() as cur:
    try:
        cur.execute(f"SELECT count(*) FROM {db_schema}.orders")
        print(f"   orders still present? {cur.fetchone()[0]} rows")
    except Exception as e:
        print(f"   🔴 orders: {str(e).splitlines()[0]}")
        conn_prod.rollback() if not conn_prod.autocommit else None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storefront Checkpoint 2: The Disaster
# MAGIC
# MAGIC Open the **DataCart Storefront** and observe the graceful degradation:
# MAGIC
# MAGIC | Page | What You'll See |
# MAGIC |------|----------------|
# MAGIC | **Home** | Top Rated still works, but Best Sellers shows "temporarily unavailable" |
# MAGIC | **Shop** | Products still browsable with stock badges and ratings |
# MAGIC | **Cart** | Your cart items are still there, but checkout shows an error |
# MAGIC | **Orders** | "Orders Service Unavailable" with a "Continue Shopping" button |
# MAGIC
# MAGIC > The storefront degrades gracefully — products are still browsable even though
# MAGIC > orders are gone. This is exactly what real customers would experience.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Create a PITR Recovery Branch
# MAGIC
# MAGIC Here's where Lakebase saves the day. We recover production's state from the timestamp we
# MAGIC recorded **before the disaster** by creating a **new branch** from that point in time using the
# MAGIC `source_branch_time` parameter. The recovery branch is a full copy of production as it was at that
# MAGIC moment, including the orders table with all its data.
# MAGIC
# MAGIC > This is the non-destructive restore model from the lecture: a **new root branch** is created,
# MAGIC > and the original production branch is unchanged. (A project supports up to **3 root branches** —
# MAGIC > delete an old one first if you hit the limit.)

# COMMAND ----------

from databricks.sdk.service.postgres import Branch, BranchSpec, Timestamp, Duration

PITR_BRANCH = "pitr-recovery"

# 5) Create the PITR recovery branch from the recorded timestamp
try:
    w.postgres.delete_branch(name=f"projects/{project_name}/branches/{PITR_BRANCH}").wait()
    print(f"🧹 Cleaned up existing branch '{PITR_BRANCH}'")
except Exception:
    pass

print(f"🔄 Creating PITR branch from production at {before_timestamp}...")
w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        source_branch_time=Timestamp(seconds=before_epoch),
        ttl=Duration(seconds=86400),  # 24-hour TTL for recovery branch
    )),
    branch_id=PITR_BRANCH,
).wait()
print(f"✅ PITR branch '{PITR_BRANCH}' created with pre-disaster data.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify data on the recovery branch
# MAGIC
# MAGIC Connect to the **recovery branch** and confirm the `orders` table is back with all its rows.
# MAGIC
# MAGIC > ✅ **Expected result:** the `orders` count and `revenue` **match the healthy numbers from Step 1**
# MAGIC > — the recovery branch has the pre-disaster data intact.

# COMMAND ----------

# 6) Verify data on the recovery branch
conn_pitr, _, _ = connect_to_branch(PITR_BRANCH)
with conn_pitr.cursor() as cur:
    cur.execute(f"SELECT count(*) FROM {db_schema}.orders")
    print(f"📦 Recovery branch has {cur.fetchone()[0]} orders.")
    cur.execute(f"SELECT COALESCE(SUM(total), 0) FROM {db_schema}.orders")
    print(f"💰 Revenue on recovery branch: ${cur.fetchone()[0]:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Restore Production
# MAGIC
# MAGIC The recovery branch has the good `orders` data, but the **storefront reads `production`** — which
# MAGIC is still missing the table. So we copy the data back into production by:
# MAGIC 1. Recreating the `orders` table on production
# MAGIC 2. Copying the rows from the recovery branch into it
# MAGIC 3. Resetting the ID sequence
# MAGIC
# MAGIC > This is easy from the SDK because we hold open connections to **both** branches at once and stream
# MAGIC > the rows across. In practice, you could also use `pg_dump`/`pg_restore` or application-level data
# MAGIC > migration.

# COMMAND ----------

# 7) Copy the recovered data back into production
#    7a: recreate the orders table on production
with conn_prod.cursor() as cur:
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.orders (
            id              SERIAL PRIMARY KEY,
            customer_id     INTEGER NOT NULL REFERENCES {db_schema}.customers(id),
            product_id      INTEGER NOT NULL REFERENCES {db_schema}.products(id),
            quantity         INTEGER NOT NULL DEFAULT 1,
            total            NUMERIC(10, 2) NOT NULL,
            currency         VARCHAR(3) NOT NULL DEFAULT 'USD',
            order_date       DATE NOT NULL DEFAULT CURRENT_DATE,
            status           VARCHAR(20) NOT NULL DEFAULT 'pending'
        )
    """)

#    7b: read rows from the recovery branch
with conn_pitr.cursor() as cur:
    cur.execute(f"""
        SELECT id, customer_id, product_id, quantity, total, currency, order_date, status
        FROM {db_schema}.orders
        ORDER BY id
    """)
    orders_data = cur.fetchall()

#    7c: insert into production and reset the sequence
with conn_prod.cursor() as cur:
    for row in orders_data:
        cur.execute(f"""
            INSERT INTO {db_schema}.orders (id, customer_id, product_id, quantity, total, currency, order_date, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, row)
    cur.execute(f"""
        SELECT setval(
            pg_get_serial_sequence('{db_schema}.orders', 'id'),
            (SELECT MAX(id) FROM {db_schema}.orders)
        )
    """)
print(f"   ✅ Copied {len(orders_data)} orders back into production.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify recovery
# MAGIC
# MAGIC Back on the **`production`** branch, confirm orders and revenue are restored.
# MAGIC
# MAGIC > ✅ **Expected result:** `orders` and `revenue` **match the healthy numbers from Step 1** 🎉 —
# MAGIC > production is fully restored.

# COMMAND ----------

# 8) Verify recovery
with conn_prod.cursor() as cur:
    cur.execute(f"SELECT count(*) FROM {db_schema}.orders")
    n = cur.fetchone()[0]
    cur.execute(f"SELECT COALESCE(SUM(total), 0) FROM {db_schema}.orders")
    rev = cur.fetchone()[0]
print("\n" + "=" * 60)
print(f"🎉 RECOVERY COMPLETE! Production has {n} orders, revenue ${rev:,.2f}.")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storefront Checkpoint 3: Recovery Complete
# MAGIC
# MAGIC Refresh the **DataCart Storefront**:
# MAGIC - Orders page is back with full order history
# MAGIC - Best Sellers on the homepage works again
# MAGIC - Checkout is functional again
# MAGIC
# MAGIC > The storefront detected the restored `orders` table within 30 seconds and automatically
# MAGIC > recovered — no redeployment needed. Production is fully operational again.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Cleanup (Optional)
# MAGIC
# MAGIC > Uncomment to clean up the PITR branch.

# COMMAND ----------

# Uncomment to clean up:
# conn_pitr.close()
# conn_prod.close()
# delete_branch_safe(PITR_BRANCH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |------|---------------|
# MAGIC | **Record timestamp** | Captured `NOW()` before the disaster as our recovery point |
# MAGIC | **Simulate disaster** | `DROP TABLE orders CASCADE` on production |
# MAGIC | **Confirm damage** | Orders gone, revenue $0, app broken |
# MAGIC | **Create PITR branch** | Branch from production at the pre-disaster timestamp |
# MAGIC | **Verify PITR data** | All 22 orders intact on the recovery branch |
# MAGIC | **Restore production** | Recreated table + copied data from PITR branch |
# MAGIC | **Verify recovery** | Production fully restored — 22 orders, revenue back |
# MAGIC
# MAGIC ### Concepts Covered
# MAGIC - **PITR** — recover to any second within the restore window, non-destructive (new branch created)
# MAGIC - **Snapshots** — planned, named restore points for proactive backups before risky operations
# MAGIC - **PITR vs. Snapshots** — use Snapshots *before* changes, use PITR *after* something goes wrong
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC 1. **Lakebase retains full history** — you can recover from any point within the retention window (default 24h)
# MAGIC 2. **PITR branches are instant** — no waiting for backup restores or point-in-time replay
# MAGIC 3. **Zero-copy snapshots** — the PITR branch doesn't duplicate data, it references the historical state
# MAGIC 4. **Non-destructive recovery** — you verify data on the branch before touching production
# MAGIC 5. **Record timestamps proactively** — monitoring and alerting help identify the right recovery point
# MAGIC
# MAGIC The hours-long, revenue-losing outage DataCart once suffered is now a seconds-long recovery — no backup hunt required.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Previous:** Lab 6 — Schema Migration | **Next:** Lab 8 — Workshop Summary
# MAGIC
# MAGIC > To provision your own Lakebase project and app outside this workshop, see the **Create Lakebase Project & App (using SDK)** notebook.

