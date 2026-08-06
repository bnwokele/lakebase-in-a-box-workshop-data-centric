# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5: Parallel Development with Branching
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC This lab introduces **Lakebase Autoscaling Branching** — a key feature that brings the agility
# MAGIC of code branching (branches, PRs, CI/CD) to your database. You'll learn the concepts behind
# MAGIC branching and apply them hands-on by simulating three developers working in parallel on
# MAGIC isolated branches.
# MAGIC
# MAGIC > **📍 DataCart's journey** — DataCart's developers all shared a single dev database that constantly
# MAGIC > drifted from production and had to be refreshed every weekend — a workaround that won't scale as the
# MAGIC > team hires more engineers. A Lakebase **dev branch** is a *point-in-time, zero-copy clone* of its
# MAGIC > parent: every developer gets an isolated, production-like database in seconds and can run breaking
# MAGIC > DDL without touching production or each other's work — the foundation for the parallel schema
# MAGIC > evolution we promote to production in the next lab.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC 1. **Explain** what database branching is and why it matters for modern development workflows
# MAGIC 2. **Understand** copy-on-write storage and how it enables instant, cost-efficient branches
# MAGIC 3. **Create** branches from production using the Databricks SDK
# MAGIC 4. **Work in parallel** on isolated branches without impacting production or other developers
# MAGIC 5. **Verify** that production remains untouched while branches diverge independently
# MAGIC
# MAGIC > **Docs**: [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches) | [API Reference](https://docs.databricks.com/api/workspace/postgres)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Why Database Branching?
# MAGIC
# MAGIC <img src="Includes/images/branching/shared-database.png"
# MAGIC      alt="Shared Development Database"
# MAGIC      width="1100">
# MAGIC
# MAGIC Through the years, **code** has evolved to be agile (branches, PRs, CI/CD), but **databases** have stayed static. They don't match how teams build software.
# MAGIC
# MAGIC Teams need databases to behave like code:
# MAGIC - Developers want **isolated environments** to test schema changes with no impact to production or other teams
# MAGIC - CI/CD processes need **fresh databases** for every test run
# MAGIC - Preview environments should reflect **real production data**
# MAGIC
# MAGIC Most databases today make all of this difficult. The default solution has always been **copying the database** — which is expensive, time-consuming, and error-prone. Teams compromise by testing against incomplete data or sharing environments.
# MAGIC
# MAGIC Lakebase, through **branching**, makes this process instant and cost-efficient.
# MAGIC
# MAGIC <img src="Includes/images/branching/branch-per-developer.png"
# MAGIC      alt="A branch per developer"
# MAGIC      width="1100">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## What is Branching?
# MAGIC
# MAGIC A branch in Lakebase is an **independent database environment** created from a parent branch. You can instantly create isolated environments for development, experimentation, or testing schema changes — without impacting production or duplicating data.
# MAGIC
# MAGIC <img src="Includes/images/branching/new-branching-overview-image.png"
# MAGIC      alt="Branching Overview"
# MAGIC      width="1100">
# MAGIC
# MAGIC ```
# MAGIC production (root branch)
# MAGIC     ├── staging (child of production)
# MAGIC     │    └── feature-test (child of staging)
# MAGIC     └── development (child of production)
# MAGIC           └── bugfix-branch (child of development)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy-on-Write Storage
# MAGIC
# MAGIC A branch shares storage with its parent through **copy-on-write**. When you create a branch, Lakebase takes the exact state (schema + data) of the parent, but **does not duplicate** the entire database. The new branch inherits both schema and data, sharing the same underlying storage. Only when data is modified in the child branch does Lakebase write new data — so parent and child diverge independently, storing only the changes.
# MAGIC
# MAGIC ```
# MAGIC production branch         child branch (at creation)
# MAGIC ┌─────────────────┐       ┌─────────────────┐
# MAGIC │  [Data A]       │◄──────│  → Data A       │  (shared)
# MAGIC │  [Data B]       │◄──────│  → Data B       │  (shared)
# MAGIC │  [Data C]       │◄──────│  → Data C       │  (shared)
# MAGIC └─────────────────┘       └─────────────────┘
# MAGIC
# MAGIC After modifying data in child branch:
# MAGIC ┌─────────────────┐       ┌─────────────────┐
# MAGIC │  [Data A]       │◄──────│  → Data A       │  (shared)
# MAGIC │  [Data B]       │       │  [Data B']      │  (changed — stored separately)
# MAGIC │  [Data C]       │◄──────│  → Data C       │  (shared)
# MAGIC └─────────────────┘       └─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Branches appear **instantly** — database size has no impact on creation time
# MAGIC - You only pay for data that actually **changes** between branches
# MAGIC - Creating branches has **no performance impact** on production

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Working with Branches
# MAGIC
# MAGIC By default, Lakebase creates a single **`production` branch** when you create a new project. This is your default branch for production data. You can create additional branches as needed.
# MAGIC
# MAGIC **By default, the production branch never scales to zero** (though this can be configured).
# MAGIC
# MAGIC #### Creating Branches
# MAGIC
# MAGIC You can create branches from the **UI** or **SDK**:
# MAGIC
# MAGIC **From the UI:**
# MAGIC 1. Navigate to your project's Branches page
# MAGIC 2. Click **New Branch**
# MAGIC 3. Enter a name, choose expiration settings, select **Current data** or **Past data**
# MAGIC 4. Click **Create**
# MAGIC
# MAGIC <img src="Includes/images/branching/create_branch_current_data_expiration.png"
# MAGIC      alt="Create Branch"
# MAGIC      width="600">
# MAGIC
# MAGIC #### Branches from Past Data
# MAGIC
# MAGIC You can create a branch from a **specific point in time** within your restore window. This is useful for:
# MAGIC - **Data recovery** — a critical table was dropped yesterday at 10:23 AM, create a branch from 10:22 AM
# MAGIC - **Auditing** — access historical data for financial reconciliations or compliance
# MAGIC
# MAGIC <img src="Includes/images/branching/create_branch_from_past_data.png"
# MAGIC      alt="Create Branch from Past Data"
# MAGIC      width="600">
# MAGIC
# MAGIC See [Point-in-time restore](https://docs.databricks.com/aws/en/oltp/projects/point-in-time-restore) for details.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Expiring vs. Non-Expiring Branches
# MAGIC
# MAGIC An **expiring branch** has an automatic deletion timestamp. When it reaches expiration, it's automatically deleted — helping manage temporary branches and reduce costs.
# MAGIC
# MAGIC Branch expiration is ideal for:
# MAGIC - **CI/CD environments** — test branches that clean up after pipeline completion
# MAGIC - **Feature development** — time-boxed branches with known deadlines
# MAGIC - **Automated testing** — ephemeral test environments created by scripts
# MAGIC
# MAGIC See [How branch expiration works](https://docs.databricks.com/aws/en/oltp/projects/manage-branches#how-branch-expiration-works) for details.
# MAGIC
# MAGIC <details>
# MAGIC <summary><strong>Other special branch types</strong></summary>
# MAGIC
# MAGIC **Protected branches** have special rules that restrict operations like deletion, reset, and archival. See [Protected branches](https://docs.databricks.com/aws/en/oltp/projects/protected-branches).
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Branch Strategies
# MAGIC
# MAGIC Common ways teams organize their branches:
# MAGIC
# MAGIC #### Production - Development - Staging
# MAGIC Your development branch is where you build new features safely. When ready, run tested schema migrations against production. Staging mirrors production data for pre-production testing.
# MAGIC
# MAGIC ```
# MAGIC production
# MAGIC ├── staging
# MAGIC └── development
# MAGIC ```
# MAGIC
# MAGIC #### Per-Developer Setup
# MAGIC Each developer gets their own branch from development. They experiment independently and apply tested migrations when ready.
# MAGIC
# MAGIC ```
# MAGIC production
# MAGIC └── development
# MAGIC     ├── dev-alice
# MAGIC     ├── dev-bob
# MAGIC     └── dev-charlie
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Hands-On: Parallel Development Scenario
# MAGIC
# MAGIC **The Challenge:**
# MAGIC DataCart has three developers that need to make **schema changes simultaneously** to support new features for the Spring Sale.
# MAGIC
# MAGIC | Developer | Team | Task |
# MAGIC |-----------|------|------|
# MAGIC | **Developer A** (you) | Loyalty Team | Add `loyalty_points` column, new `loyalty_members` table and `reviews` table |
# MAGIC | Developer B | Global Team | Add `exchange_rates` table + convert `currency` to a FK |
# MAGIC | Developer C | Performance Team | Add indexes to `products` for Spring Sale traffic surge |
# MAGIC
# MAGIC In this lab **you'll play Developer A** and take the Loyalty Team's work through its own isolated branch. Developers B and C would be doing the same thing on their own branches at the same time — that parallelism is exactly what branching unlocks.
# MAGIC
# MAGIC Traditional database workflows create bottlenecks:
# MAGIC - Schema changes can create friction (Developer A's DDL changes can break Developer B's code when sharing the **same copy** of the database)
# MAGIC - Creating isolated environments is expensive (spinning up a full replica means paying for a second instance, waiting 15+ minutes for snapshot restore)
# MAGIC - Testing against synthetic datasets fails to catch edge cases that only exist in real-world data
# MAGIC
# MAGIC **The Lakebase Solution: Branching**
# MAGIC Each developer creates an isolated **branch** — a zero-copy snapshot of production. They work independently, validate changes, and then perform migrations on production after validation. The production branch is never touched during development.
# MAGIC
# MAGIC **[Technical Blog](https://community.databricks.com/t5/technical-blog/lakebase-branching-meets-docker-the-migration-safety-net-i-wish/ba-p/149945) to learn more about the great benefits of Lakebase Branching from an ex-backend engineer**

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Two ways to do this lab — pick one
# MAGIC
# MAGIC The hands-on work in this lab is presented **two ways**. They accomplish exactly the same thing —
# MAGIC pick whichever fits how you like to work:
# MAGIC
# MAGIC | | **Path A — Interactive (UI + SQL editor)** | **Path B — SDK ("Run All")** |
# MAGIC |---|---|---|
# MAGIC | How | Create the branch in the Lakebase UI and paste SQL into the branch's SQL editor | Run the notebook top-to-bottom; `psycopg2` + the Databricks SDK do it for you |
# MAGIC | Best for | Seeing and clicking through each step yourself | A fast, scripted walkthrough / seeing the SDK calls |
# MAGIC | Where | **Path A — Interactive** section below | **Path B — SDK** section at the bottom |
# MAGIC
# MAGIC > Both paths are equivalent and idempotent. **Do just one** — or run Path A first and use Path B
# MAGIC > later to see the programmatic equivalent.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Developer A — Loyalty Team
# MAGIC
# MAGIC **Goal:** Add a `loyalty_points` column to the `users` table and create a new `loyalty_members` table to support DataCart's Spring Sale loyalty program.

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is a loyalty program — and why is DataCart adding one?
# MAGIC
# MAGIC A **loyalty program** rewards customers for repeat business. Instead of treating every purchase as
# MAGIC a one-off, the store tracks how much each customer spends over time, converts that into **points**,
# MAGIC and groups customers into **tiers** (Bronze → Silver → Gold → Platinum). Higher tiers unlock perks —
# MAGIC early access, better discounts, free shipping — which gives customers a reason to keep coming back.
# MAGIC
# MAGIC **Why DataCart wants this now.** The **Spring Sale** is coming, and DataCart's best customers are
# MAGIC exactly the ones it most wants to bring back for it. A loyalty program lets DataCart:
# MAGIC - **Reward its highest-value customers** ahead of the sale (e.g. early access for Gold/Platinum)
# MAGIC - **Lift retention and repeat purchases** — points give customers a reason to return
# MAGIC - **Segment customers by value** so marketing can target the right offer to the right tier
# MAGIC
# MAGIC That's the Loyalty Team's job: add points tracking to the existing customer data and stand up a
# MAGIC tiered membership table — all on an isolated branch, without touching production.

# COMMAND ----------

# MAGIC %md
# MAGIC ### How the underlying tables build the `loyalty_members` table
# MAGIC
# MAGIC The loyalty program isn't built from scratch — it's **derived from data DataCart already has**.
# MAGIC Two existing tables feed into the new `loyalty_members` table:
# MAGIC
# MAGIC | Source | Role in the loyalty program |
# MAGIC |--------|-----------------------------|
# MAGIC | **`orders`** (existing) | The record of what each customer has spent. This is the *source of value* — points are earned from real purchase history. |
# MAGIC | **`customers`** (existing) | The people we're enrolling. It gains a new **`loyalty_points`** column. |
# MAGIC
# MAGIC The build happens in three steps (this is exactly what the SQL/SDK cells below do):
# MAGIC
# MAGIC 1. **Add `loyalty_points` to `customers`, and backfill it from `orders`.** Each customer's points
# MAGIC    are the sum of their order totals, rounded down: `SUM(FLOOR(orders.total))` grouped by customer.
# MAGIC    A customer who has spent \$1,240 across their orders ends up with `loyalty_points = 1240`.
# MAGIC
# MAGIC 2. **Enroll customers into `loyalty_members`.** Every customer with `loyalty_points > 0` becomes a
# MAGIC    member. Their point total is copied into `total_earned`, and each member is linked back to the
# MAGIC    customer by `email` (a foreign key to `customers.email`).
# MAGIC
# MAGIC 3. **Assign a tier from the points.** A `CASE` expression maps the point total to a tier:
# MAGIC
# MAGIC    | Tier | Points threshold |
# MAGIC    |------|------------------|
# MAGIC    | **Platinum** | ≥ 3000 |
# MAGIC    | **Gold** | ≥ 1500 |
# MAGIC    | **Silver** | ≥ 500 |
# MAGIC    | **Bronze** | anything above 0 |
# MAGIC
# MAGIC Developer A also creates a **`reviews`** table (product ratings from beta testers) in the same
# MAGIC branch — it's the other half of the Loyalty Team's work and powers the storefront's star ratings.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### The loyalty data model, visualized
# MAGIC
# MAGIC The diagram below shows the full set of Developer A's changes: how `orders` and `customers` feed the
# MAGIC new `loyalty_points` column and the `loyalty_members` table, plus the standalone `reviews` table.
# MAGIC
# MAGIC <img src="Includes/images/branching/loyalty-data-model.png"
# MAGIC      alt="How orders and customers build the loyalty_members table, plus the reviews table"
# MAGIC      width="1100">

# COMMAND ----------

# MAGIC %md
# MAGIC # Path A — Interactive (UI + SQL editor)
# MAGIC
# MAGIC *One of two equivalent paths — see "Two ways to do this lab" above. For the SDK version, jump to
# MAGIC **Path B — SDK ("Run All")** at the bottom.*
# MAGIC
# MAGIC ## Step 1: Create Developer A's branch in the Lakebase UI
# MAGIC
# MAGIC Developer A starts by creating an isolated branch from `production`. This is a **zero-copy
# MAGIC snapshot** — no data is duplicated on disk; the branch only diverges as changes are made.
# MAGIC
# MAGIC **Create the branch in the UI:**
# MAGIC 1. Open the **Lakebase project UI** (the project link was printed in Lab 1).
# MAGIC 2. Select the **`production`** branch, then click **Create branch** (or the **+** in the branch list).
# MAGIC 3. Name the new branch **`dev-loyalty-reviews`**.
# MAGIC 4. Leave `production` as the **source branch** and create it. It's ready in a few seconds.
# MAGIC
# MAGIC > This mirrors how a developer would spin up their own isolated database on demand. Branches can
# MAGIC > also be set to expire automatically (e.g. a 48-hour TTL) so short-lived feature work cleans
# MAGIC > itself up — matching the CI/CD pattern discussed above.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Run Developer A's changes in the Lakebase SQL editor
# MAGIC
# MAGIC Now that the `dev-loyalty-reviews` branch exists, let's do Developer A's work **the way a developer
# MAGIC actually would** — by connecting a SQL editor to the branch and running the migration there.
# MAGIC
# MAGIC **How to open the SQL editor on your branch:**
# MAGIC 1. In the Lakebase project UI, select the **`dev-loyalty-reviews`** branch (not `production`).
# MAGIC 2. Open the branch's **SQL editor** and run each block below, in order.
# MAGIC
# MAGIC ### 1. Add the `loyalty_points` column and backfill it from order history
# MAGIC ```sql
# MAGIC ALTER TABLE ecommerce.customers
# MAGIC ADD COLUMN IF NOT EXISTS loyalty_points INT NOT NULL DEFAULT 0;
# MAGIC
# MAGIC UPDATE ecommerce.customers u
# MAGIC SET loyalty_points = (
# MAGIC     SELECT COALESCE(SUM(FLOOR(o.total)::INT), 0)
# MAGIC     FROM ecommerce.orders o WHERE o.customer_id = u.id
# MAGIC );
# MAGIC
# MAGIC -- See the result
# MAGIC SELECT id, name, loyalty_points
# MAGIC FROM ecommerce.customers
# MAGIC ORDER BY loyalty_points DESC
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC > ✅ **Expected result:** the top 10 customers, each now showing a non-zero `loyalty_points` value
# MAGIC > (highest first) — the column was added and backfilled from order history.
# MAGIC
# MAGIC ### 2. Create the `loyalty_members` table and enroll customers by tier
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce.loyalty_members (
# MAGIC     id              SERIAL PRIMARY KEY,
# MAGIC     email           VARCHAR(255) NOT NULL REFERENCES ecommerce.customers(email),
# MAGIC     tier            VARCHAR(20) NOT NULL DEFAULT 'Bronze'
# MAGIC         CHECK (tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')),
# MAGIC     enrolled_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
# MAGIC     total_earned    INT         NOT NULL DEFAULT 0
# MAGIC );
# MAGIC
# MAGIC INSERT INTO ecommerce.loyalty_members (email, tier, enrolled_at, total_earned)
# MAGIC SELECT
# MAGIC     email,
# MAGIC     CASE
# MAGIC         WHEN loyalty_points >= 3000 THEN 'Platinum'
# MAGIC         WHEN loyalty_points >= 1500 THEN 'Gold'
# MAGIC         WHEN loyalty_points >= 500  THEN 'Silver'
# MAGIC         ELSE 'Bronze'
# MAGIC     END,
# MAGIC     NOW(),
# MAGIC     loyalty_points
# MAGIC FROM ecommerce.customers
# MAGIC WHERE loyalty_points > 0
# MAGIC ON CONFLICT (id) DO NOTHING;
# MAGIC
# MAGIC -- See the enrolled members
# MAGIC SELECT lm.id, u.name, lm.tier, lm.total_earned AS points
# MAGIC FROM ecommerce.loyalty_members lm
# MAGIC JOIN ecommerce.customers u ON u.email = lm.email
# MAGIC ORDER BY lm.total_earned DESC
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC > ✅ **Expected result:** up to 10 enrolled members with a `tier` (Bronze → Platinum) assigned by
# MAGIC > their points — the `loyalty_members` table was created and populated.
# MAGIC
# MAGIC ### 3. Create the `reviews` table and seed it with beta-tester reviews
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce.reviews (
# MAGIC     id SERIAL PRIMARY KEY,
# MAGIC     product_id INT NOT NULL REFERENCES ecommerce.products(id),
# MAGIC     customer_id INT NOT NULL REFERENCES ecommerce.customers(id),
# MAGIC     rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
# MAGIC     comment TEXT,
# MAGIC     review_date TIMESTAMP DEFAULT NOW()
# MAGIC );
# MAGIC
# MAGIC -- Seed a spread of beta-tester reviews. Several products get 2+ reviews so the
# MAGIC -- storefront's "Top Rated" section (which needs ≥2 reviews per product) lights up.
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
# MAGIC
# MAGIC -- See the seeded reviews
# MAGIC SELECT product_id, COUNT(*) AS review_count, ROUND(AVG(rating), 1) AS avg_rating
# MAGIC FROM ecommerce.reviews
# MAGIC GROUP BY product_id
# MAGIC ORDER BY product_id;
# MAGIC ```
# MAGIC
# MAGIC > ✅ **Expected result:** ~12 products with reviews, several showing a `review_count` of 2 or 3 and
# MAGIC > an `avg_rating` — the `reviews` table is created and populated.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Prove the branch is isolated from production
# MAGIC
# MAGIC This is the payoff of branching. Run the first query **on the `dev-loyalty-reviews` branch**, then
# MAGIC switch your SQL editor to the **`production`** branch and run the second — the column exists on your
# MAGIC branch but **not** on production.
# MAGIC
# MAGIC **On the `dev-loyalty-reviews` branch — the new column exists:**
# MAGIC ```sql
# MAGIC SELECT column_name
# MAGIC FROM information_schema.columns
# MAGIC WHERE table_schema = 'ecommerce'
# MAGIC   AND table_name  = 'customers'
# MAGIC   AND column_name = 'loyalty_points';
# MAGIC ```
# MAGIC
# MAGIC > ✅ **Expected result:** **1 row** — `loyalty_points`. The column is present on the branch.
# MAGIC
# MAGIC **Switch the editor to the `production` branch — the change is NOT there:**
# MAGIC ```sql
# MAGIC SELECT column_name
# MAGIC FROM information_schema.columns
# MAGIC WHERE table_schema = 'ecommerce'
# MAGIC   AND table_name  = 'customers'
# MAGIC   AND column_name = 'loyalty_points';
# MAGIC ```
# MAGIC
# MAGIC > ✅ **Expected result:** **0 rows** — production is untouched. The change lives only on the branch.
# MAGIC
# MAGIC > **Why doesn't the storefront app change?** The DataCart app connects only to the **`production`**
# MAGIC > branch, and your work lives on `dev-loyalty-reviews`. That's exactly the point: your in-progress
# MAGIC > schema changes are invisible to production (and to the app) until you deliberately promote them —
# MAGIC > which is what **Lab 6** does. The storefront won't light up loyalty/reviews until then.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Path B — SDK ("Run All")
# MAGIC
# MAGIC *The second of two equivalent paths — see "Two ways to do this lab" near the top.*
# MAGIC
# MAGIC These cells reproduce everything in Path A through `psycopg2` and the SDK — **including creating the
# MAGIC branch** — so you can run the notebook end-to-end instead of using the UI + SQL editor. It's
# MAGIC self-contained: the setup cells below install dependencies and define the branch helpers, then
# MAGIC create the branch and run all of Developer A's changes. Safe to run more than once (all DDL is
# MAGIC idempotent); if you already created `dev-loyalty-reviews` in the UI, the create step just reuses it.
# MAGIC
# MAGIC > If you already completed **Path A** above, you don't need to run this — it's the same work the
# MAGIC > other way. Run it if you'd like to see the SDK equivalent.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 0: Install Dependencies & Configure Helpers (SDK path)

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

def delete_branch_safe(branch_id, max_retries=6, wait_between=30):
    """
    Delete a branch, retrying if the endpoint is still reconciling.

    Args:
        branch_id: Branch name (e.g. "dev-readonly")
        max_retries: Max number of retry attempts (default 6)
        wait_between: Seconds to wait between retries (default 30)
    """
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

print("🔧 print_table, connect_to_branch() and delete_branch_safe() helpers defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task A-1: Create Branch `dev-loyalty-reviews` (SDK)
# MAGIC
# MAGIC The SDK equivalent of the UI branch-creation step above. `ttl=Duration(seconds=172800)` makes this
# MAGIC a **48-hour expiring branch** that cleans itself up automatically.

# COMMAND ----------

from databricks.sdk.service.postgres import Branch, BranchSpec, Duration

BRANCH_NAME = "dev-loyalty-reviews"

# Fixed configuration (used by connect_to_branch())
db_schema = "ecommerce"
min_cu = 0.5
max_cu = 4.0
suspend_timeout_seconds = 1800

# Clean up from previous runs
try:
    w.postgres.delete_branch(name=f"projects/{project_name}/branches/{BRANCH_NAME}").wait()
    print(f"🧹 Cleaned up existing branch '{BRANCH_NAME}'")
except Exception:
    pass

# Create your feature branch
print(f"\n🔄 Creating branch '{BRANCH_NAME}' from production...")
w.postgres.create_branch(
    parent=f"projects/{project_name}",
    branch=Branch(spec=BranchSpec(
        source_branch=prod_branch_name,
        ttl=Duration(seconds=172800)  # 48-hour TTL
    )),
    branch_id=BRANCH_NAME
).wait()
print(f"✅ Branch '{BRANCH_NAME}' created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task A-2: Add `loyalty_points` Column to `Customers`
# MAGIC
# MAGIC Developer A runs their DDL migration on the isolated `dev-loyalty-reviews` branch. This change is **invisible to production** and to the other developers' branches.

# COMMAND ----------

# connect to dev-loyalty-reviews branch
conn_loyalty, _, _ = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

print("🔧 Developer A: Adding loyalty features to 'dev-loyalty-reviews' branch...\n")

# Add loyalty_points column to users
with conn_loyalty.cursor() as cur:
    cur.execute(f"""
    ALTER TABLE {db_schema}.customers
    ADD COLUMN IF NOT EXISTS loyalty_points INT NOT NULL DEFAULT 0;
""")

# Backfill some loyalty points based on order history
with conn_loyalty.cursor() as cur:
    cur.execute(f"""
    UPDATE {db_schema}.customers u
    SET loyalty_points = (
        SELECT COALESCE(SUM(FLOOR(o.total)::INT), 0)
        FROM {db_schema}.orders o WHERE o.customer_id = u.id
    );
""")

print("✅ Added 'loyalty_points' column and backfilled from order history.")

# Show updated users
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
conn_loyalty.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task A-3: Create the `loyalty_members` Table
# MAGIC
# MAGIC Developer A also creates an entirely new table — this schema change exists **only on the `dev-loyalty-reviews` branch**.

# COMMAND ----------

# connect to dev-loyalty-reviews branch
conn_loyalty, _, _ = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

# Create loyalty_members table for customers with enough points
with conn_loyalty.cursor() as cur:
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {db_schema}.loyalty_members (
        id              SERIAL PRIMARY KEY,
        email           VARCHAR(255) NOT NULL REFERENCES {db_schema}.customers(email),
        tier            VARCHAR(20) NOT NULL DEFAULT 'Bronze'
            CHECK (tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')),
        enrolled_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
        total_earned    INT         NOT NULL DEFAULT 0
    );
""")

# Enroll customers with enough points
with conn_loyalty.cursor() as cur:
    cur.execute(f"""
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
""")

with conn_loyalty.cursor() as cur:
    cur.execute(f"""
    SELECT lm.id, u.name, lm.tier, lm.total_earned AS points
    FROM {db_schema}.loyalty_members lm
    JOIN {db_schema}.customers u ON u.email = lm.email
    ORDER BY lm.total_earned DESC
    LIMIT 10;
""")
    cols, rows = [d[0] for d in cur.description], cur.fetchall()
print("✅ Created 'loyalty_members' table and enrolled customers:")
print_table(cols, rows)
conn_loyalty.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task A-4: Seed Product Reviews on the Branch
# MAGIC
# MAGIC Developer A also seeds customer reviews collected from beta testers. These reviews
# MAGIC will be promoted to production along with the loyalty features in Lab 6 — giving
# MAGIC the storefront star ratings and customer feedback.

# COMMAND ----------

# connect to dev-loyalty-reviews branch
conn_loyalty, _, _ = connect_to_branch(BRANCH_NAME)

# COMMAND ----------

import random
random.seed(42)

with conn_loyalty.cursor() as cur:
    # Create reviews table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.reviews (
            id SERIAL PRIMARY KEY,
            product_id INT NOT NULL REFERENCES {db_schema}.products(id),
            customer_id INT NOT NULL REFERENCES {db_schema}.customers(id),
            rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
            comment TEXT,
            review_date TIMESTAMP DEFAULT NOW()
        );
    """)

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

print(f"✅ Created reviews table and seeded {len(reviews)} product reviews on dev-loyalty-reviews branch")
conn_loyalty.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task A-5: Verify Production Branch is UNCHANGED
# MAGIC
# MAGIC This is the critical test. Connect to the **`production`** branch and confirm that:
# MAGIC 1. `customers` still has **no** `loyalty_points` column
# MAGIC
# MAGIC This proves that branches provide true **schema isolation** — exactly like the copy-on-write model described above.

# COMMAND ----------

# connect to production branch
conn_prod, conn_host, conn_endpoint = connect_to_branch('production')

# COMMAND ----------

print("🔍 Checking production branch schema...\n")

# Check columns on users table in production
with conn_prod.cursor() as cur:
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
# MAGIC ## Summary
# MAGIC
# MAGIC Developer A created an isolated **branch** to accomplish their tasks in a production-like environment. They worked independently, tested their changes, and the production branch was never touched during development.
# MAGIC
# MAGIC | Developer | Team | Branch | Task |
# MAGIC |-----------|------|--------|------|
# MAGIC | Developer A | Loyalty Team | `dev-loyalty-reviews` | Add `loyalty_points` column + `loyalty_members` + `reviews` tables |
# MAGIC
# MAGIC > On a real team, Developers B and C would spin up their own branches (e.g. `modify-orders`, `add-index`) from the same production branch at the same time — each fully isolated — while Developer A works on `dev-loyalty-reviews`. That's the whole point of branching: parallel work with zero conflicts.
# MAGIC
# MAGIC **Key concepts demonstrated:**
# MAGIC - **Copy-on-write** — branches are instant, no data duplication
# MAGIC - **Expiring branches** — 48-hour TTL for automatic cleanup
# MAGIC - **Schema isolation** — changes on a branch don't affect production or other branches
# MAGIC - **Per-developer setup** — the branching strategy pattern in action
# MAGIC
# MAGIC DataCart's shared-dev-database drift and weekend refreshes are gone — every developer now works on an isolated branch that's production-like from the first second.
# MAGIC
# MAGIC **Next:** In Lab 6, we'll promote Developer A's changes to production using the **Migration Replay** pattern, and explore the **Schema Diff** tool.

