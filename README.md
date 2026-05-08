# Lakebase-in-a-Box Workshop

This hands-on workshop introduces Databricks Lakebase — a fully managed, serverless PostgreSQL database built on open architecture that decouples compute from storage and demonstrates how to leverage its unique capabilities to integrate operational and analytical workloads on a single governed platform.

You will step into the role of a database engineer at DataCart, a rapidly growing e-commerce platform. The stakes are high: the "Spring Sale" launch is weeks away, and the data team needs to wire up data flows between Lakebase and Unity Catalog, then evolve the OLTP schema for new features (loyalty program, product reviews) — all while ensuring the production site stays bulletproof and analytics consumers keep working.

## Core Modules

| # | Notebook | Type | Description |
|---|---|---|---|
| 0 | `0 Workshop Introduction` | Lecture | Workshop overview, Lakebase architecture, and the DataCart scenario |
| 1.1 | `1.1 Lab - Discover and Seed the Lakebase Project` | Lab | Discover the bundle-deployed project, OAuth connection, and e-commerce schema seeding (customers, products, orders) |
| 2.1 | `2.1 Lab - Roles Permissions and Connect Storefront` | Lab | Workspace vs. database permission layers; grant the storefront's service principal access and bring it online |
| 3.1 | `3.1 Lab - Reverse ETL with Synced Tables (UC to Lakebase)` | Lab | Create a promotions Delta table in Unity Catalog and sync it to Lakebase; sale badges appear on the storefront |
| 4.1 | `4.1 Lab - Register Lakebase in Unity Catalog` | Lab | Register Lakebase as a UC foreign catalog and run a federated join of live OLTP × Delta marketing data |
| 5.1 | `5.1 Lab - Lakehouse Sync (Lakebase to UC)` | Lab | Continuously mirror Lakebase tables to Delta in UC; run analytics with zero OLTP load |
| 6.1 | `6.1 Lab - Parallel Development with Branching` | Lab | Three developers work in parallel on isolated branches (loyalty features, multi-currency support, performance indexes) |
| 6.2 | `6.2 Lab - Schema Migration to Production` | Lab | Promote validated schema changes from a feature branch to production by replaying DDL; verify changes propagate to UC |
| 6.3 | `6.3 Lab - Branch Reset` | Lab | Detect production drift, reset a branch to match parent state, and re-test migrations |
| 7.1 | `7.1 Lab - Point in Time Recovery and Snapshots` | Lab | Simulate an accidental `DROP TABLE` and recover using PITR; observe federation and sync pipelines healing automatically |
| 8 | `8 Lecture - Monitoring` | Lecture | How to monitor your Lakebase instance and interpret the metrics on the Lakebase monitoring page |
| 9 | `9 Lecture - Connect Apps to Lakebase` | Lecture | How to connect external apps to Lakebase |


## DataCart Storefront App

A customer-facing e-commerce web application (React + FastAPI) that **evolves in real time** as each lab modifies the database. Located in `datacart-storefront/`.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│              DataCart Storefront App                │
│  ┌─────────────┐        ┌────────────────────────┐  │
│  │ React UI    │  HTTP  │  FastAPI Backend       │  │
│  │ (Vite SPA)  │───────▶│  /api/shop/*           │  │
│  │             │        │  /api/cart/*           │  │
│  │ - Home      │        │  /api/orders/*         │  │
│  │ - Shop      │        └───────────┬────────────┘  │
│  │ - Product   │                    │ psycopg3      │
│  │ - Cart      │                    │ OAuth tokens  │
│  │ - Orders    │                    ▼               │
│  └─────────────┘        ┌────────────────────────┐  │
│                         │  Lakebase (PostgreSQL) │  │
│                         │  ecommerce schema      │  │
│                         │  production branch     │  │
│                         └────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

| Feature | Appears After |
|---------|--------------|
| Products, stock badges, cart, orders | Lab 1.1 + 2.1 |
| Sale badges, discount prices, promo deals | Lab 3.1 |
| (UC analytics surface lights up — no storefront change) | Lab 4.1 + 5.1 |
| Star ratings, reviews | Lab 6.2 |
| Loyalty tier badge, points, "Earn X pts" | Lab 6.2 |
| Priority badges, verified badge | Lab 6.3 |
| Graceful degradation during disaster | Lab 7.1 |

### Prerequisites

- Databricks workspace with Lakebase & Databricks Apps support
- Unity Catalog enabled (required for Labs 4.1 and 5.1)
- A SQL warehouse (any size) for the federated queries in Lab 4.1
- Databricks CLI v0.229.0+ authenticated with a profile

## Setup Steps

### Step 1: Deploy the Storefront and Lakebase Project via DABs

The `datacart-storefront/` folder is a Databricks Asset Bundle that deploys the app, the Lakebase Autoscaling project, and the setup job in one shot.

**Before deploying**, update the target environments in `databricks.yml` to match your workspace:

```yaml
# datacart-storefront/databricks.yml — update the profile in each target to your CLI profile
targets:
  dev:
    default: true
    mode: development
    workspace:
      profile: <your-profile>    # ← Change this to your CLI profile

  workshop:
    mode: production
    workspace:
      profile: <your-profile>    # ← Change this to your CLI profile
      root_path: /Workspace/Users/<your-email>/.bundle/${bundle.name}/${bundle.target}
```

> **How to find your profile**: Run `databricks auth profiles` to list available profiles.
> If you haven't set one up, run `databricks auth login --host <workspace-url> --profile <profile-name>` first.

Then deploy and provision:

```bash
cd datacart-storefront

# Validate the bundle
databricks bundle validate -t workshop

# Deploy app + setup job to the workspace
databricks bundle deploy -t workshop

# Provision the Lakebase Autoscaling project (idempotent — safe to re-run)
databricks bundle run setup_lakebase_project -t workshop

# Start the storefront app
databricks bundle run datacart_storefront -t workshop
```

### Step 2: Confirm the Lakebase resource binding

The first time you run an app that declares a `postgres` resource (see `app.yaml`), the Databricks Apps platform asks you to confirm which Lakebase project to bind to. Pick **`datacart-data-centric`**. After that, the platform auto-injects `PGHOST`, `PGUSER`, `PGPORT`, etc. as env vars on every restart.

> **Why this matters**: Databricks Apps run as a service principal (SP). The SP needs a Postgres role in Lakebase to authenticate. Adding the database as a resource handles role creation and credential injection automatically.

### Step 3: Run Lab 1.1 to seed the schema

Open **`1.1 Lab - Discover and Seed the Lakebase Project`** in the workspace. This:

1. Discovers the bundle-deployed Lakebase project (`datacart-data-centric`)
2. Seeds 5 tables: customers, products, inventory, orders, order_items
3. Tours `pg_catalog`, `information_schema`, and `pg_stat_statements`

### Step 4: Run Lab 2.1 to grant SP permissions

Open **`2.1 Lab - Roles Permissions and Connect Storefront`**. The storefront will show "Loading…" until this lab grants the app's service principal access to the `ecommerce` schema. Once complete, the storefront populates with products and a working cart.

### Step 5: Go through the rest of the workshop!

Run the remaining labs in order: 3.1 → 4.1 → 5.1 → 6.1 → 6.2 → 6.3 → 7.1 → 8 → 9.


## Workshop Flow — Storefront Evolution

The storefront **auto-detects schema changes** every 30 seconds. No redeployment is needed — just run the lab and refresh the browser.

### After 1.1 Lab - Setup & 2.1 Lab - Connect Storefront to Lakebase

**Database:** 5 tables (customers, products, inventory, orders, order_items). No reviews yet.

**Storefront shows:**
- Products with prices and stock badges (In Stock / Low Stock / Out of Stock)
- Shopping cart with checkout
- Order history with status badges
- **No star ratings** — reviews table doesn't exist yet
- **No loyalty features** — loyalty tables don't exist yet

### After Lab 3.1 — Reverse ETL with Synced Tables

**Database change:** A `promotions` Delta table is created in Unity Catalog and synced to Lakebase via a synced table pipeline. First synced to a `dev-promotions` branch for validation, then promoted to the `production` branch. The synced table appears as `promotions_synced_prod` (or `promotions`) in the `ecommerce` Postgres schema.

**Important — SP permissions for synced tables:** After the sync completes, you must re-grant the app SP access to the new table. Synced tables are created by the Lakebase sync pipeline (a different internal role), so `ALTER DEFAULT PRIVILEGES` from Lab 2.1 does **not** cover them. Lab 3.1 Step 7 handles this with:
```sql
GRANT ALL ON ALL TABLES IN SCHEMA ecommerce TO "<SP_CLIENT_ID>";
```

**Storefront shows (promotions go live!):**
- **Homepage** — New "Spring Sale Deals" section with promoted products
- **Product cards** — Red sale badges (e.g., "SPRING SALE -20%", "FLASH SALE -45%") on promoted products
- **Product cards** — Original prices crossed out with sale prices in red
- **Product detail** — Promotion alert showing badge, discount %, and sale price
- **Cart** — Promoted items show the discounted sale price

> Key demo point: The marketing team updated a Delta table in Unity Catalog. The synced
> table pipeline pushed the data to Lakebase. The storefront detected the new table and
> rendered promotions. **Zero application code changes required.**

### After Lab 4.1 — Register Lakebase in Unity Catalog

**Database change:** A new UC foreign catalog `lakebase_datacart` is registered against the Lakebase production branch. A small `main.datacart_demo.marketing_campaigns` Delta table is created for the federated join scenario.

**What's queryable now:**
- Live Lakebase data via `lakebase_datacart.ecommerce.*` from any SQL warehouse
- A federated join of live `orders` (Lakebase) with `marketing_campaigns` (Delta) — zero ETL

**Storefront shows:** No change. The analytics surface gets the upgrade.

### After Lab 5.1 — Lakehouse Sync (Lakebase to UC)

**Database change:** A Lakehouse Sync pipeline continuously mirrors `orders`, `customers`, and `order_items` from Lakebase to Delta tables under `main.datacart_uc`.

**What's queryable now:**
- Delta replicas of OLTP tables — heavy analytical aggregations run on photon, no OLTP load
- A live insert demo proves the Lakebase → Delta loop is closed

**Storefront shows:** No change. BI / ML consumers can now hit the lakehouse side.

### After Lab 6.1 — Parallel Development

**Database:** No changes to production. Three feature branches are created:
- `dev-loyalty-reviews` — loyalty_points column, loyalty_members table, and **reviews table**
- `modify-orders` — exchange_rates table, currency FK migration
- `add-index` — price index on products

**Storefront shows:** No change — all work is on isolated branches. The synced flows from Labs 3.1 and 5.1 keep targeting production.

### After Lab 6.2 — Schema Migration to Production

**Database changes on production:**
- `customers` table gets `loyalty_points` column (backfilled from order history)
- `loyalty_members` table created (customers enrolled by tier: Bronze/Silver/Gold/Platinum)
- `reviews` table created and seeded with ~80 product reviews

**Storefront shows (new features appear!):**
- **Navbar** — Alice Smith's loyalty tier badge (e.g., "Gold") and points count
- **Homepage** — Purple "Loyalty Program Active!" banner below the hero
- **Homepage** — "Top Rated" section appears (now that reviews exist)
- **Product cards** — Star ratings and review counts appear
- **Product cards** — "Earn X pts" labels below prices
- **Product detail** — Full customer reviews section with stars and comments
- **Cart** — "You'll earn X loyalty points" summary with tier badge
- **Checkout** — Awards loyalty points after placing an order

**UC also reflects the change:** the foreign catalog (Lab 4.1) sees the new column on the next query; Lakehouse Sync (Lab 5.1) propagates it to Delta on the next sync cycle.

### After Lab 6.3 — Branch Reset

**Database changes on production:**
- `customers` table gets `email_verified` BOOLEAN column (~1/3 verified)
- `orders` table gets `priority` VARCHAR column (high/medium/normal based on total)

**Storefront shows (more features appear!):**
- **Navbar** — Green "Verified" badge appears next to the loyalty tier
- **Orders page** — Each order now shows a priority badge (high = red, medium = amber, normal = gray)

**UC also reflects the change:** federation and sync both pick up the new columns, same pattern as 6.2.

### During Lab 7.1 — PITR (The Disaster)

**Database change:** `DROP TABLE orders CASCADE` — drops both `orders` and `order_items`. Tables that **survive**: customers, products, inventory, reviews, loyalty_members.

**Storefront shows (graceful degradation):**

| Page | What Happens |
|------|-------------|
| **Homepage** | Top Rated still works. Best Sellers shows "temporarily unavailable" |
| **Shop** | Products still browsable with stock badges, ratings, and "Earn X pts" |
| **Product Detail** | Reviews still visible |
| **Cart** | Items still there, but checkout shows "temporarily unavailable" error |
| **Orders** | Full-page "Orders Service Unavailable" with "Continue Shopping" button |

**UC also reacts:**
- Foreign catalog queries on `orders` fail with "relation does not exist"
- Lakehouse Sync pipeline tracking `orders` errors out

> Key demo point: the storefront **degrades gracefully** — products are still browsable
> even though orders are gone. This is what real customers would experience. The downstream
> data flows respond honestly: federation breaks (live read), sync stalls (last replica still queryable in Delta).

### After Lab 7.1 — PITR Recovery

**Database change:** Orders table recreated from PITR branch, data restored.

**Storefront shows (recovery):**
- Orders page is back with full order history
- Best Sellers works again
- Checkout is functional again
- **Priority badges are gone** — PITR restored to a point before Lab 6.3

**UC also recovers:** the foreign catalog query works again immediately; the Lakehouse Sync pipeline resumes (or one click to "Resume" if it gave up).

### After Lab 7.1 — Post-Recovery Migrations

**Database change:** Lab 6.3 migrations re-applied (email_verified + priority columns).

**Storefront shows (full restore):**
- Priority badges are back on the Orders page
- Verified badge is back in the navbar
- All features from the entire workshop are restored

> Key demo point: PITR recovers data to a point in time. Post-recovery, you re-apply
> any migrations that happened after the recovery point — just like replaying commits
> after a git reset. Downstream UC flows heal alongside production.

## Troubleshooting

### "Store Unavailable" error on homepage
- The Lakebase endpoint may be suspended (scale-to-zero). Wait 10-20 seconds and refresh.
- Check that the ecommerce schema exists (run `1.1 Lab - Discover and Seed the Lakebase Project` first).

### "Loading..." forever
- Hit `<app-url>/api/dbtest` to check connectivity.
- If `PGHOST` shows `NOT SET`: the app was **not bound to the Lakebase project**. Open the app's
  Resources tab and confirm the binding to `datacart-data-centric`, then click **Deploy** to restart.
- If `db_connected: false` with "password authentication failed": the database resource
  was not added, or the SP role was not auto-created. Re-bind the resource and redeploy.
- If `db_connected: true` with `schema_error`: the SP needs schema grants — run Lab 2.1.

### 500 errors on product pages
- Check app logs at `<app-url>/logz`
- Verify the SP has PostgreSQL roles on the ecommerce schema (Lab 2.1)

### Spring Sale Deals section not appearing (after Lab 3.1)
- Check `/api/features` — if `promotions_active` is `false`, the SP can't see the synced table.
- Re-run `GRANT ALL ON ALL TABLES IN SCHEMA ecommerce TO "<SP_CLIENT_ID>";` as the project owner
  (Lab 3.1 Step 7). Synced tables are created by the sync pipeline, not your user, so
  `ALTER DEFAULT PRIVILEGES` doesn't apply to them.
- The storefront checks for both `promotions_synced_prod` and `promotions` table names.

### Federated query errors with "connection refused" (Lab 4.1)
- Foreign catalog connections require Lakehouse Federation to be enabled on your SQL warehouse.
- Use a serverless SQL warehouse if you don't have classic warehouses configured for federation.

### Lakehouse Sync option not visible in the UI (Lab 5.1)
- Lakehouse Sync is gated by region and feature flag — confirm the **Sync to Unity Catalog** option
  is visible on your project's page. If not, ask your Databricks contact to enable the feature on
  this workspace.

### Cart/checkout not working
- Cart is stored in-memory on the app server. It resets on deploy.
- Checkout requires sufficient inventory stock.

## Logs

Access application logs at: `<app-url>/logz`

## Databricks Documentation

- [Lakebase Overview](https://docs.databricks.com/aws/en/oltp/)
- [Manage Branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)
- [Point-in-Time Recovery](https://docs.databricks.com/aws/en/oltp/projects/point-in-time-restore)
- [Connect to Your Database](https://docs.databricks.com/aws/en/oltp/projects/connect)
- [Postgres Roles](https://docs.databricks.com/aws/en/oltp/projects/postgres-roles)
- [Register a Lakebase database in Unity Catalog](https://docs.databricks.com/aws/en/oltp/projects/register-uc)
- [Lakehouse Sync](https://docs.databricks.com/aws/en/oltp/projects/lakehouse-sync)
- [API Reference](https://docs.databricks.com/api/workspace/postgres)
