# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 6: Schema Changes — Feature Branch to Production
# MAGIC
# MAGIC In Lab 5, we created a feature branch `dev-loyalty-reviews`, added the `loyalty_points` column to the `customers` table, created new `loyalty_members` and `reviews` tables. Now we'll promote those changes to production using the **Migration Replay** pattern.
# MAGIC
# MAGIC > **Cross-flow note.** When this migration lands on production, the **Lakebase CDF** pipeline from
# MAGIC > Lab 4 evolves the Delta target schema on its next sync cycle — so the new column / tables show
# MAGIC > up in the lakehouse with no extra work.
# MAGIC
# MAGIC This lab also introduces two important branch management features: **Schema Diff** for comparing branches before migration, and **Branch Reset** for refreshing branches from their parent.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC 1. **Use Schema Diff** to compare branch schemas before promoting changes
# MAGIC 2. **Replay validated migrations** from a feature branch to production
# MAGIC 3. **Understand branch reset** and when to use it
# MAGIC 4. **Verify** that schema changes were successfully promoted
# MAGIC
# MAGIC ## How It Works
# MAGIC ```
# MAGIC production ─────────────────── replay migration ────── production (with loyalty_points, loyalty_members, and reviews)
# MAGIC        \                           ↑
# MAGIC         └── feature/dev-loyalty-reviews   │
# MAGIC              1. ALTER TABLE        │
# MAGIC              2. Backfill data      │
# MAGIC              3. Validate ──────────┘
# MAGIC              4. Delete branch
# MAGIC ```
# MAGIC
# MAGIC > **Docs**: [Compare branch schemas](https://docs.databricks.com/aws/en/oltp/projects/manage-branches#compare-branch-schemas) | [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)
# MAGIC
# MAGIC **[Technical Blog](https://community.databricks.com/t5/technical-blog/lakebase-branching-meets-docker-the-migration-safety-net-i-wish/ba-p/149945) to learn more about the great benefits of Lakebase Branching from an ex-backend engineer**

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## New Workflow When Branching is Possible
# MAGIC
# MAGIC With database branching, your database changes follow the same branch → change → test → PR
# MAGIC workflow you already use for code. Each task gets its own branch database, you iterate until
# MAGIC the solution is satisfactory and tests pass, then open a PR that carries both the code and the
# MAGIC database migration script.
# MAGIC
# MAGIC <img src="Includes/images/branching/new-workflow-with-branching.png"
# MAGIC      alt="New workflow when branching is possible"
# MAGIC      width="500">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Schema Diff: Comparing Branches Before Migration
# MAGIC
# MAGIC Before promoting changes to production, you should always review what changed. The **Schema Diff** tool lets you compare schemas between two branches in a side-by-side view, highlighting added, removed, or modified database objects (tables, columns, indexes, constraints).
# MAGIC
# MAGIC It is designed for:
# MAGIC - **Pre-migration validation** — ensure only intended changes are applied
# MAGIC - **Development tracking** — understand the evolution of your database structure
# MAGIC - **Drift detection** — check consistency across development, staging, and production branches
# MAGIC
# MAGIC ### How to Use Schema Diff
# MAGIC
# MAGIC 1. Navigate to a **child branch** overview page in the Lakebase UI
# MAGIC 2. In the **Parent branch** section, click **Schema diff**
# MAGIC
# MAGIC ![child-branch-overview.png](Includes/images/branching/child-branch-overview-schema-diff-button.png)
# MAGIC
# MAGIC 3. Select the **base branch** for comparison (defaults to parent)
# MAGIC 4. Select the **database** to compare
# MAGIC 5. Select the **branch** to compare against (defaults to current child)
# MAGIC 6. Click **Compare**
# MAGIC
# MAGIC ### Understanding the Results
# MAGIC
# MAGIC - **Red lines** show what was removed or changed from the base branch
# MAGIC - **Green lines** show what was added or changed in the compare branch
# MAGIC
# MAGIC ![schema-diff-results.png](Includes/images/branching/schema-diff-lecture.png)
# MAGIC
# MAGIC If no differences exist, you see a success message confirming the schemas are in sync:
# MAGIC
# MAGIC ![no-schema-diff-results.png](Includes/images/branching/schema-diff-no-diffs.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Resetting a Branch to Match Parent State
# MAGIC
# MAGIC When working with branches, you might need to update your working branch to the latest data from its parent. When you **reset a branch**, the data and schema are completely replaced with the latest from the parent.
# MAGIC
# MAGIC **When to use reset:**
# MAGIC - When a child branch is too far out of date with the parent and you have no schema changes to preserve
# MAGIC - You want a clean, instant refresh of the data
# MAGIC
# MAGIC ![reset_branch.png](Includes/images/branching/reset_branch_to_parent_state.png)
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px;">Key Points About Branch Reset</strong>
# MAGIC   <div style="color:#333;">
# MAGIC     <ul>
# MAGIC       <li>You can only reset to the <strong>latest data</strong> from the parent (not a point in time)</li>
# MAGIC       <li>This is a <strong>complete overwrite</strong>, not a merge — local changes are lost</li>
# MAGIC       <li>Existing connections are temporarily interrupted but re-establish automatically</li>
# MAGIC       <li>Root branches (like production) <strong>cannot be reset</strong> — they have no parent</li>
# MAGIC       <li>For point-in-time recovery, use <strong>point-in-time restore</strong> instead (creates a new branch)</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Two ways to do this lab — pick one
# MAGIC
# MAGIC The hands-on work in this lab is presented **two ways**. They accomplish exactly the same thing —
# MAGIC pick whichever fits how you like to work:
# MAGIC
# MAGIC | | **Path A — Interactive (UI + SQL editor)** | **Path B — SDK ("Run All")** |
# MAGIC |---|---|---|
# MAGIC | How | Use the Schema Diff UI and paste SQL into the `production` SQL editor | Run the notebook top-to-bottom; `psycopg2` + the Databricks SDK do it for you |
# MAGIC | Best for | Watching the storefront light up as you promote each change | A fast, scripted walkthrough / seeing the SDK calls |
# MAGIC | Where | **Path A — Interactive** section below | **Path B — SDK** section at the bottom |
# MAGIC
# MAGIC > Both paths are equivalent and idempotent (`IF NOT EXISTS` / `ON CONFLICT`). **Do just one** — or
# MAGIC > run Path A first and use Path B later to see the programmatic equivalent.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Path A — Interactive (UI + SQL editor)
# MAGIC
# MAGIC *One of two equivalent paths — see "Two ways to do this lab" above. For the SDK version, jump to
# MAGIC **Path B — SDK ("Run All")** at the bottom.*
# MAGIC
# MAGIC Do the migration the way a developer actually would — in the **Lakebase SQL editor** and the
# MAGIC **Schema Diff UI**. This whole section is copy/paste SQL and UI clicks; no Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Compare Schemas Using Schema Diff
# MAGIC
# MAGIC Now is the time to use the **Schema Diff** tool we discussed above. Before replaying the migration on production, visually compare what changed.
# MAGIC
# MAGIC 1. Open the **Lakebase project UI** (the project link was printed in Lab 1).
# MAGIC 2. Navigate to the `dev-loyalty-reviews` branch
# MAGIC 3. Click the **Schema diff** button to see the differences vs production
# MAGIC
# MAGIC You should see:
# MAGIC - **Green lines** for the new `loyalty_points` column on `customers`
# MAGIC - **Green lines** for the new `loyalty_members` and `reviews` tables
# MAGIC
# MAGIC > **Docs**: [Compare branch schemas](https://docs.databricks.com/aws/en/oltp/projects/manage-branches#compare-branch-schemas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify production is untouched (before promoting)
# MAGIC
# MAGIC Developer A's work so far lives only on the `dev-loyalty-reviews` branch. Confirm production
# MAGIC doesn't have the new column **yet**. Open the SQL editor on the **`production`** branch and run:
# MAGIC ```sql
# MAGIC SELECT column_name
# MAGIC FROM information_schema.columns
# MAGIC WHERE table_schema = 'ecommerce'
# MAGIC   AND table_name  = 'customers'
# MAGIC   AND column_name = 'loyalty_points';
# MAGIC ```
# MAGIC
# MAGIC > ✅ **Expected result:** **0 rows** — production has not been migrated yet.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Promote the migration to production
# MAGIC
# MAGIC This time we promote to **`production`** — and because the DataCart storefront reads production,
# MAGIC you'll watch the app light up loyalty and reviews **live**.
# MAGIC
# MAGIC **How to run it:**
# MAGIC 1. Open the **Lakebase project UI** and select the **`production`** branch.
# MAGIC 2. Open the branch's **SQL editor** and run the block below.
# MAGIC 3. Keep the storefront app open in another tab — within ~30 seconds it re-detects the schema and
# MAGIC    loyalty tiers, "Earn X pts" labels, and star ratings appear (see the checkpoint below).
# MAGIC
# MAGIC > 💡 **Path B — SDK ("Run All")** at the end runs the exact same DDL via `psycopg2`. The SQL is
# MAGIC > idempotent (`IF NOT EXISTS` / `ON CONFLICT`), so running both is harmless.
# MAGIC
# MAGIC ```sql
# MAGIC -- Add loyalty_points column to customers
# MAGIC ALTER TABLE ecommerce.customers
# MAGIC     ADD COLUMN IF NOT EXISTS loyalty_points INT NOT NULL DEFAULT 0;
# MAGIC
# MAGIC -- Backfill loyalty points based on order history
# MAGIC UPDATE ecommerce.customers u
# MAGIC     SET loyalty_points = (
# MAGIC         SELECT COALESCE(SUM(FLOOR(o.total)::INT), 0)
# MAGIC         FROM ecommerce.orders o WHERE o.customer_id = u.id
# MAGIC     );
# MAGIC
# MAGIC -- Create loyalty_members table for customers with enough points
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce.loyalty_members (
# MAGIC         id              SERIAL PRIMARY KEY,
# MAGIC         email           VARCHAR(255) NOT NULL REFERENCES ecommerce.customers(email),
# MAGIC         tier            VARCHAR(20) NOT NULL DEFAULT 'Bronze'
# MAGIC             CHECK (tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')),
# MAGIC         enrolled_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
# MAGIC         total_earned    INT         NOT NULL DEFAULT 0
# MAGIC     );
# MAGIC
# MAGIC -- Enroll customers with enough points
# MAGIC INSERT INTO ecommerce.loyalty_members (email, tier, enrolled_at, total_earned)
# MAGIC     SELECT
# MAGIC         email,
# MAGIC         CASE
# MAGIC             WHEN loyalty_points >= 3000 THEN 'Platinum'
# MAGIC             WHEN loyalty_points >= 1500 THEN 'Gold'
# MAGIC             WHEN loyalty_points >= 500  THEN 'Silver'
# MAGIC             ELSE 'Bronze'
# MAGIC         END,
# MAGIC         NOW(),
# MAGIC         loyalty_points
# MAGIC     FROM ecommerce.customers
# MAGIC     WHERE loyalty_points > 0
# MAGIC     ON CONFLICT (id) DO NOTHING;
# MAGIC
# MAGIC -- Create reviews table (product ratings from beta testers)
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce.reviews (
# MAGIC     id SERIAL PRIMARY KEY,
# MAGIC     product_id INT NOT NULL REFERENCES ecommerce.products(id),
# MAGIC     customer_id INT NOT NULL REFERENCES ecommerce.customers(id),
# MAGIC     rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
# MAGIC     comment TEXT,
# MAGIC     review_date TIMESTAMP DEFAULT NOW()
# MAGIC );
# MAGIC
# MAGIC -- Seed beta-tester reviews. Several products get 2+ reviews so the storefront's
# MAGIC -- "Top Rated" section (which needs ≥2 reviews per product) lights up.
# MAGIC INSERT INTO ecommerce.reviews (id, product_id, customer_id, rating, comment, review_date) VALUES
# MAGIC     (1,  1,  3,  5, 'Great product, highly recommend!',        '2024-01-05'),
# MAGIC     (2,  1,  12, 4, 'Solid build quality, very happy.',        '2024-01-11'),
# MAGIC     (3,  1,  27, 5, 'Best purchase I''ve made this year.',      '2024-01-19'),
# MAGIC     (4,  2,  8,  4, 'Exceeded my expectations.',               '2024-01-07'),
# MAGIC     (5,  2,  33, 5, 'Fast shipping and excellent quality.',    '2024-01-22'),
# MAGIC     (6,  3,  5,  3, 'Decent product for the price.',           '2024-01-09'),
# MAGIC     (7,  3,  41, 4, 'Would buy again in a heartbeat.',         '2024-01-15'),
# MAGIC     (8,  5,  17, 5, 'Exceeded my expectations.',               '2024-01-03'),
# MAGIC     (9,  5,  52, 4, 'Great product, highly recommend!',        '2024-01-25'),
# MAGIC     (10, 7,  22, 2, 'Not as described, somewhat disappointed.','2024-01-13'),
# MAGIC     (11, 7,  60, 4, 'Does what it''s supposed to do.',          '2024-01-18'),
# MAGIC     (12, 9,  14, 5, 'Solid build quality, very happy.',        '2024-01-06'),
# MAGIC     (13, 9,  38, 5, 'Best purchase I''ve made this year.',      '2024-01-21'),
# MAGIC     (14, 12, 45, 4, 'Fast shipping and excellent quality.',    '2024-01-10'),
# MAGIC     (15, 12, 71, 3, 'Average quality, nothing special.',       '2024-01-24'),
# MAGIC     (16, 15, 9,  5, 'Would buy again in a heartbeat.',         '2024-01-08'),
# MAGIC     (17, 15, 63, 4, 'Great product, highly recommend!',        '2024-01-17'),
# MAGIC     (18, 18, 29, 1, 'Quality could be better.',                '2024-01-12'),
# MAGIC     (19, 18, 55, 3, 'Okay but could be improved.',             '2024-01-20'),
# MAGIC     (20, 21, 4,  5, 'Exceeded my expectations.',               '2024-01-04'),
# MAGIC     (21, 21, 48, 4, 'Solid build quality, very happy.',        '2024-01-26'),
# MAGIC     (22, 25, 36, 5, 'Best purchase I''ve made this year.',      '2024-01-14'),
# MAGIC     (23, 25, 77, 4, 'Fast shipping and excellent quality.',    '2024-01-23'),
# MAGIC     (24, 30, 19, 4, 'Would buy again in a heartbeat.',         '2024-01-16')
# MAGIC ON CONFLICT (id) DO NOTHING;
# MAGIC
# MAGIC -- Keep the id sequence in sync with the rows we just inserted
# MAGIC SELECT setval(pg_get_serial_sequence('ecommerce.reviews', 'id'),
# MAGIC               (SELECT MAX(id) FROM ecommerce.reviews));
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify the migration landed on production
# MAGIC
# MAGIC Still in the **`production`** SQL editor, confirm the new column and tables now exist:
# MAGIC ```sql
# MAGIC -- loyalty_points now exists on customers
# MAGIC SELECT column_name
# MAGIC FROM information_schema.columns
# MAGIC WHERE table_schema = 'ecommerce'
# MAGIC   AND table_name  = 'customers'
# MAGIC   AND column_name = 'loyalty_points';
# MAGIC
# MAGIC -- the new tables exist
# MAGIC SELECT table_name
# MAGIC FROM information_schema.tables
# MAGIC WHERE table_schema = 'ecommerce'
# MAGIC   AND table_name IN ('loyalty_members', 'reviews')
# MAGIC ORDER BY table_name;
# MAGIC ```
# MAGIC
# MAGIC > ✅ **Expected result:**
# MAGIC > - First query: **1 row** — `loyalty_points` (the column was added).
# MAGIC > - Second query: **2 rows** — `loyalty_members` and `reviews` (both new tables exist).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storefront Checkpoint: Loyalty Program & Reviews Go Live!
# MAGIC
# MAGIC Open the **DataCart Storefront** and observe the new features appearing in real time:
# MAGIC
# MAGIC 1. **Navbar** — Alice Smith's loyalty tier badge appears (Bronze/Silver/Gold/Platinum) with points count
# MAGIC 2. **Home page** — A purple "Loyalty Program Active!" banner appears below the hero
# MAGIC 3. **Products** — Star ratings now appear on every product card
# MAGIC 4. **Product detail** — Full customer reviews section with star ratings and comments
# MAGIC 5. **Product cards** — "Earn X pts" labels appear below prices
# MAGIC 6. **Cart** — Shows how many loyalty points you'll earn with your order
# MAGIC
# MAGIC > The storefront auto-detects schema changes every 30 seconds. No redeployment needed!
# MAGIC > If you don't see changes immediately, wait 30 seconds and refresh.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Path B — SDK ("Run All")
# MAGIC
# MAGIC *The second of two equivalent paths — see "Two ways to do this lab" near the top.*
# MAGIC
# MAGIC These cells reproduce everything in Path A through `psycopg2` and the SDK, so you can run the
# MAGIC notebook end-to-end instead of using the SQL editor. It's self-contained: the setup cells install
# MAGIC dependencies and define the branch helpers, then it connects to the branches itself, replays the
# MAGIC **same idempotent DDL** on production, and seeds reviews.
# MAGIC
# MAGIC > If you already completed **Path A** above, you don't need to run this — it's the same work the
# MAGIC > other way (safe to run anyway: `IF NOT EXISTS` / `ON CONFLICT`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Run Setup (SDK path)

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
# MAGIC
# MAGIC This function is used by all scenario notebooks (01-05) to connect to a specific branch.
# MAGIC It handles endpoint discovery, waiting, and OAuth token generation.

# COMMAND ----------

def connect_to_branch(branch_id, wait_seconds=300):
    """
    Connect to a Lakebase branch endpoint.
    Automatically creates a compute endpoint if none exists.

    Args:
        branch_id: Branch name (e.g. "dev-readonly", "feature-loyalty-tier")
        wait_seconds: Max seconds to wait for endpoint to become ready (default 300)

    Returns:
        tuple: (connection, host, endpoint_name)
    """
    from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, Duration as Dur

    branch_full = f"projects/{project_name}/branches/{branch_id}"

    # Check if an endpoint already exists
    endpoints = list(w.postgres.list_endpoints(parent=branch_full))

    if not endpoints:
        # Create a compute endpoint for this branch
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

    # Wait for the endpoint host to be available
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

    # Generate OAuth token and connect
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

print("🔧 print_table and connect_to_branch() helpers defined.")

# COMMAND ----------

db_schema = "ecommerce"
BRANCH_NAME = "dev-loyalty-reviews"

# Fixed configuration
min_cu = 0.5
max_cu = 4.0
suspend_timeout_seconds = 1800

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Connect to dev-loyalty-reviews branch

# COMMAND ----------

# connect to dev-loyalty-reviews branch
conn_loyalty, _, _ = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

# Show updated customers table with loyalty_points column
with conn_loyalty.cursor() as cur:
    cur.execute(f"""
    SELECT id, name, loyalty_points
    FROM {db_schema}.customers
    ORDER BY loyalty_points DESC
    LIMIT 10;
""")
    cols, rows = [d[0] for d in cur.description], cur.fetchall()
print("\n🏆 Users with loyalty points (dev-loyalty-reviews branch):")
print_table(cols, rows)

# COMMAND ----------

# Show loyalty_members table in dev-loyalty-reviews branch
with conn_loyalty.cursor() as cur:
    cur.execute(f"""
    SELECT lm.id, u.name, lm.tier, lm.total_earned AS points
    FROM {db_schema}.loyalty_members lm
    JOIN {db_schema}.customers u ON u.email = lm.email
    ORDER BY lm.total_earned DESC
    LIMIT 10;
""")
    cols, rows = [d[0] for d in cur.description], cur.fetchall()
print("✅ 'loyalty_members' table and enrolled customers (dev-loyalty-reviews branch):")
print_table(cols, rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify Production is Untouched
# MAGIC
# MAGIC The schema change only exists on the branch. Production still has the original schema.

# COMMAND ----------

# connect to production branch
conn, conn_host, conn_endpoint = connect_to_branch('production')

# COMMAND ----------

print("🔍 Checking production branch schema...\n")

# Check columns on customers table in production
with conn.cursor() as cur:
    cur.execute(f"""
    SELECT column_name, data_type, column_default, table_schema, table_name
    FROM information_schema.columns
    WHERE table_schema = '{db_schema}' AND table_name = 'customers'
    ORDER BY ordinal_position;
""")
    prod_columns = [row[0] for row in cur.fetchall()]

print(f"📋 Production branch columns: {prod_columns}")
print(f"   Has loyalty_points? {'loyalty_points' in prod_columns}")
print("\n" + "=" * 60)
print("🎯 RESULT: 'loyalty_points' and 'loyalty_members' exist ONLY")
print("   in 'dev-loyalty-reviews'. Production schema is untouched!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC Picture of Schema differences

# COMMAND ----------

# Print direct link to the branch in the Lakebase UI
branch_obj = w.postgres.get_branch(name=f"projects/{project_name}/branches/{BRANCH_NAME}")
branch_uid = branch_obj.uid
workspace_host = w.config.host.rstrip("/")
lakebase_url = f"{workspace_host}/lakebase/projects/{branch_uid }"
print(f"🔗 Open the branch in the Lakebase UI and click 'Schema diff':")
print(f"   {lakebase_url}/branches/{branch_uid}")

# COMMAND ----------

# The migration script — idempotent and replayable
MIGRATION_SQL = f"""
-- Add loyalty_points column to users
ALTER TABLE {db_schema}.customers
    ADD COLUMN IF NOT EXISTS loyalty_points INT NOT NULL DEFAULT 0;

-- Backfill some loyalty points based on order history
UPDATE {db_schema}.customers u
    SET loyalty_points = (
        SELECT COALESCE(SUM(FLOOR(o.total)::INT), 0)
        FROM {db_schema}.orders o WHERE o.customer_id = u.id
    );

-- Create loyalty_members table for customers with enough points
CREATE TABLE IF NOT EXISTS {db_schema}.loyalty_members (
        id              SERIAL PRIMARY KEY,
        email           VARCHAR(255) NOT NULL REFERENCES {db_schema}.customers(email),
        tier            VARCHAR(20) NOT NULL DEFAULT 'Bronze'
            CHECK (tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')),
        enrolled_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
        total_earned    INT         NOT NULL DEFAULT 0
    );

-- Enroll customers with enough points
INSERT INTO {db_schema}.loyalty_members (email, tier, enrolled_at, total_earned)
    SELECT
        email,
        CASE
            WHEN loyalty_points >= 3000 THEN 'Platinum'
            WHEN loyalty_points >= 1500 THEN 'Gold'
            WHEN loyalty_points >= 500  THEN 'Silver'
            ELSE 'Bronze'
        END,
        NOW(),
        loyalty_points
    FROM {db_schema}.customers
    WHERE loyalty_points > 0
    ON CONFLICT (id) DO NOTHING;

-- Create reviews table (product ratings from beta testers)
CREATE TABLE IF NOT EXISTS {db_schema}.reviews (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES {db_schema}.products(id),
    customer_id INT NOT NULL REFERENCES {db_schema}.customers(id),
    rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    comment TEXT,
    review_date TIMESTAMP DEFAULT NOW()
);
"""

print("✅ Migration Script Created!")

# COMMAND ----------

# Replay the exact same migration on production
with conn.cursor() as cur:
    cur.execute(MIGRATION_SQL)

print("✅ Migration replayed on production!")

# Verify on production
with conn.cursor() as cur:
    cur.execute(f"""
    SELECT column_name, data_type, column_default, table_schema, table_name
    FROM information_schema.columns
    WHERE table_schema = '{db_schema}' AND table_name = 'customers'
    ORDER BY ordinal_position;
""")
    prod_columns = [row[0] for row in cur.fetchall()]

print(f"📋 Production branch columns: {prod_columns}")
print(f"   Has loyalty_points? {'loyalty_points' in prod_columns}")
print("\n" + "=" * 60)
print("🎯 RESULT: 'loyalty_points' and 'loyalty_members' exist now exist in Production")
print("=" * 60)

print(f"\n🎉 Schema change successfully promoted to production!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Seed Product Reviews on Production
# MAGIC
# MAGIC The reviews table was created by the migration. Now seed it with the beta tester
# MAGIC reviews data that Developer A prepared on the branch.

# COMMAND ----------

import random
random.seed(42)

with conn.cursor() as cur:
    positive_comments = [
        "Great product, highly recommend!",
        "Exceeded my expectations.",
        "Fast shipping and excellent quality.",
        "Would buy again in a heartbeat.",
        "Best purchase I've made this year.",
        "Solid build quality, very happy.",
    ]
    neutral_comments = [
        "Decent product for the price.",
        "Does what it's supposed to do.",
        "Average quality, nothing special.",
        "Okay but could be improved.",
    ]
    negative_comments = [
        "Not as described, somewhat disappointed.",
        "Quality could be better.",
        "Arrived late but product is okay.",
    ]

    reviews = []
    reviewed_pairs = set()
    for _ in range(80):
        product_id = random.randint(1, 50)
        customer_id = random.randint(1, 100)
        if (product_id, customer_id) in reviewed_pairs:
            continue
        reviewed_pairs.add((product_id, customer_id))
        rating = random.choices([1, 2, 3, 4, 5], weights=[5, 8, 15, 35, 37])[0]
        if rating >= 4:
            comment = random.choice(positive_comments)
        elif rating == 3:
            comment = random.choice(neutral_comments)
        else:
            comment = random.choice(negative_comments)
        day_offset = random.randint(0, 270)
        review_date = f"2024-01-{1 + (day_offset % 28):02d}"
        reviews.append((product_id, customer_id, rating, comment, review_date))

    cur.executemany(
        f"INSERT INTO {db_schema}.reviews (product_id, customer_id, rating, comment, review_date) "
        f"VALUES (%s, %s, %s, %s, %s)",
        reviews
    )

print(f"✅ Seeded {len(reviews)} product reviews on production!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Cleanup — Delete the Feature Branch
# MAGIC
# MAGIC The feature branch has served its purpose. You can safely delete it, or let TTL handle it.
# MAGIC
# MAGIC > This cell is skipped by default. Remove `%skip` below to delete the branch now.

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC feature_conn.close()
# MAGIC
# MAGIC delete_branch_safe(BRANCH_NAME)
# MAGIC
# MAGIC # List remaining branches
# MAGIC branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))
# MAGIC print(f"\n📋 Remaining branches:")
# MAGIC for b in branches:
# MAGIC     branch_id = b.name.split("/branches/")[-1]
# MAGIC     print(f"   • {branch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | What Happened |
# MAGIC |---|---|
# MAGIC | **Schema Diff** | Compared branch vs. production to review changes before promoting |
# MAGIC | **Validate** | Verified schema, data integrity, tier distribution on the branch |
# MAGIC | **Isolate** | Confirmed production was untouched during development |
# MAGIC | **Promote** | Replayed the same idempotent DDL on production |
# MAGIC | **Seed** | Populated reviews data on production |
# MAGIC | **Cleanup** | Feature branch can be deleted or will expire via TTL |
# MAGIC
# MAGIC ### The Migration Replay Pattern
# MAGIC ```
# MAGIC 1. Write idempotent DDL (ALTER TABLE ... IF NOT EXISTS, etc.)
# MAGIC 2. Test on branch -> validate -> fix if needed -> re-test
# MAGIC 3. Once validated, replay the DDL on production
# MAGIC 4. Delete the branch
# MAGIC ```
# MAGIC
# MAGIC ### Concepts Covered
# MAGIC - **Schema Diff** — visual comparison of branch schemas for pre-migration validation, drift detection, and change documentation
# MAGIC - **Branch Reset** — how to refresh a child branch with the latest parent data (complete overwrite, not a merge)
# MAGIC - **Migration Replay** — the pattern of testing DDL on a branch and replaying it on production
# MAGIC
# MAGIC **Next:** In Lab 7, we'll explore **Point-in-Time Recovery & Snapshots**.

