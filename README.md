# Lakebase-in-a-Box Workshop — Data-Centric

This hands-on workshop introduces Databricks Lakebase — a fully managed, serverless PostgreSQL
database built on an open architecture that decouples compute from storage — and shows how it unifies
your operational database with the lakehouse so analytics, applications, and OLTP all sit on one
governed platform.

You step into the role of a **database engineer** at **DataCart**, a rapidly growing e-commerce
platform preparing for its "Spring Sale" launch. The first goal is to get **data flowing in every
direction** between Lakebase and Unity Catalog; once those flows are in place, you safely evolve the
OLTP schema (loyalty program, product reviews) and prove the platform is resilient to disaster.

This is the **data-centric** edition. It covers the same Lakebase capabilities as the app-centric
edition but leads with the data-movement story (Reverse ETL and Lakebase CDF) before branching and
schema evolution.

## Notebooks

Run the notebooks in order. `0.1` provisions everything; the labs build on each other.

| # | Notebook | Description |
|---|----------|-------------|
| 0 | `0 Workshop Introduction` | Workshop overview, Lakebase architecture, and the DataCart scenario |
| 0.1 | `0.1 Lab - Create Lakebase Project & App (using SDK)` | Provision the Lakebase project **and** the storefront app (and their binding) via the Databricks SDK |
| 1 | `1 Lab - Discover and Seed the Lakebase Project` | Discover the project, connect over OAuth, and seed the `ecommerce` schema (customers, products, inventory, orders, order_items) |
| 2 | `2 Lab - Roles Permissions and Connect Storefront` | Workspace vs. database permission layers; grant the app's service principal Postgres access and bring the storefront online |
| 3 | `3 Lab - Reverse ETL with Synced Tables (UC to Lakebase)` | Push Spring Sale promotions from a Unity Catalog Delta table into Lakebase via managed synced tables |
| 4 | `4 Lab - Lakebase CDF (Lakebase to UC)` | Continuously mirror live Lakebase tables into Delta in Unity Catalog for high-throughput analytics |
| 5 | `5 Lab - Parallel Development with Branching` | Zero-copy branching; three developers evolve the schema in parallel on isolated branches (loyalty + reviews, multi-currency FK, performance indexes) |
| 6 | `6 Lab - Schema Migration to Production` | Promote validated changes from a feature branch to production via Migration Replay; Schema Diff and Branch Reset concepts |
| 7 | `7 Lab - Point in Time Recovery and Snapshots` | Simulate an accidental `DROP TABLE` and recover production with PITR; snapshots vs. PITR |
| 8 | `8 Workshop Summary` | End-to-end recap of everything you built |
| — | `CLEAN_UP - PLEASE RUN AT THE END!` | **Run last.** Deletes the Lakebase project and the storefront app to tear down all workshop resources |

> **⚠️ Unity Catalog target (Labs 3 & 4).** These labs write to a Unity Catalog catalog you
> control. Near the top of each, set:
> ```python
> UC_CATALOG = "<your-catalog-here>"
> ```
> to a catalog you can create schemas in (you need `CREATE SCHEMA` on it). The lab creates the
> `ecommerce` / `lakebase_to_lakehouse` schemas inside it.

## DataCart Storefront App

A customer-facing e-commerce web application (React + FastAPI) that **evolves in real time** as each
lab modifies the database. Located in `datacart-storefront/`.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│              DataCart Storefront App                  │
│  ┌─────────────┐        ┌────────────────────────┐  │
│  │ React UI    │  HTTP  │  FastAPI Backend        │  │
│  │ (Vite SPA)  │───────▶│  /api/shop/*            │  │
│  │             │        │  /api/cart/*            │  │
│  │ - Home      │        │  /api/orders/*          │  │
│  │ - Shop      │        └───────────┬────────────┘  │
│  │ - Product   │                    │ psycopg        │
│  │ - Cart      │                    │ OAuth tokens   │
│  │ - Orders    │                    ▼                │
│  └─────────────┘        ┌────────────────────────┐  │
│                         │  Lakebase (PostgreSQL)  │  │
│                         │  ecommerce schema       │  │
│                         │  production branch      │  │
│                         └────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

The storefront **auto-detects schema changes every ~30 seconds** — no redeployment needed. Just run a
lab and refresh the browser.

| Feature appears | After |
|-----------------|-------|
| Products, stock badges, cart, orders | Lab 1 + Lab 2 (once the SP has access) |
| Sale badges, discount prices, "Spring Sale Deals" | Lab 3 (Reverse ETL) |
| Analytics surface (no storefront change) | Lab 4 (Lakebase CDF) |
| Loyalty tier badge, points, "Earn X pts", star ratings, reviews | Lab 6 (schema promoted to production) |
| Graceful degradation, then recovery | Lab 7 (PITR disaster → restore) |

### Setup

The recommended path is the **`0.1` SDK notebook**, which creates the Lakebase project, the storefront
app, and the resource binding for you. After running it, the only manual step is pointing the app at
its source code and clicking **Deploy** (see the notebook's final section).

<details>
<summary><strong>Alternative: manual / DABs app setup</strong></summary>

The `datacart-storefront/` folder includes an `app.yaml` and a `databricks.yml` bundle config.

**app.yaml** — set the endpoint/project (do **not** hardcode `PGHOST`/`PGUSER`/`PGDATABASE`; those are
injected when you add the Lakebase database as an app resource):
```yaml
env:
  - name: LAKEBASE_PROJECT
    value: "<project-name>"      # lakebase-workshop-<your-user-id>
  - name: DB_SCHEMA
    value: "ecommerce"
resources:
  - name: postgres
    type: postgres
```

**Add the Lakebase resource before the first deploy** (Compute → Apps → Create App → Add Resource →
Database → your Lakebase project → *Can connect*). This injects the connection env vars on deploy.

**Deploy via DABs:**
```bash
cd datacart-storefront
# set your CLI profile in databricks.yml targets first
databricks bundle validate
databricks bundle deploy --target dev
```
</details>

## Troubleshooting

- **"Store Unavailable" / "Loading…" forever** — The Lakebase endpoint may be scaled to zero; wait
  ~15s and refresh. Hit `<app-url>/api/dbtest` to check connectivity. If `PGHOST` is `NOT SET`, the app
  was not redeployed after adding the database resource — redeploy.
- **`db_connected: false` (password auth failed)** — the SP role wasn't created; ensure the Lakebase
  resource is added (Lab 2 / setup), then redeploy.
- **500 errors / missing features after a lab** — the SP may need grants on newly created tables. Labs
  that add tables re-grant the SP; re-run that grant step as the project owner if needed.
- **Sale deals not appearing (after Lab 3)** — synced tables are created by the sync pipeline, so
  `ALTER DEFAULT PRIVILEGES` doesn't cover them; re-run the `GRANT ALL ON ALL TABLES IN SCHEMA ecommerce
  TO "<SP_CLIENT_ID>";` step from Lab 3.
- **Logs:** `<app-url>/logz`

## Documentation

- [Lakebase Overview](https://docs.databricks.com/aws/en/oltp/)
- [Manage Branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)
- [Point-in-Time Recovery](https://docs.databricks.com/aws/en/oltp/projects/point-in-time-restore)
- [Connect to Your Database](https://docs.databricks.com/aws/en/oltp/projects/connect)
- [Postgres Roles](https://docs.databricks.com/aws/en/oltp/projects/postgres-roles)
- [API Reference](https://docs.databricks.com/api/workspace/postgres)
