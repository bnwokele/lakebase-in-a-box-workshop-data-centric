# Workshop Setup — Data-Centric Edition

## Overview

This setup guide is for the **data-centric** workshop variant. It uses Databricks Asset Bundles
(DABs) to deploy both the DataCart Storefront app and the Lakebase Autoscaling project before
labs start, so workshop time isn't burned on UI clicks.

## Prerequisites

- **Databricks Workspace**: any FE-VM serverless-stable workspace (or any workspace with
  Lakebase Autoscaling support)
- **Databricks CLI**: v0.229.0+ authenticated with a profile (default profile name in this
  workshop: `fe-vm-ben` — change in `databricks.yml` if yours is different)
- **Unity Catalog**: enabled in your workspace (required for Labs 4.1 and 5.1)
- **A SQL warehouse**: needed for the federated queries in Lab 4.1 (any size)

> **Note:** the React frontend is pre-built and included in `frontend/dist/`. No Node.js or npm
> is required for setup — the bundle deploys the static assets directly.

## What the Bundle Deploys

```
datacart-storefront/                  ← bundle root (databricks.yml)
├── resources/
│   ├── datacart_storefront.app.yml   →  Databricks App: datacart-storefront
│   └── lakebase_setup.job.yml        →  Job: setup_lakebase_project
└── notebooks/
    └── setup_lakebase_project.py     ← runs once; creates the Lakebase project
                                          named "datacart-data-centric"
                                          (idempotent — safe to re-run)
```

The bundle does **not** create the Postgres schema or seed any tables. That stays in Lab 1.1
where attendees can read and run the SQL themselves — it's a teaching moment, not setup busywork.

## Step 1: Deploy the Bundle

From the workshop root:

```bash
cd datacart-storefront
databricks bundle deploy -t workshop -p fe-vm-ben
```

Validates the bundle, uploads files, and creates two resources in your workspace:

- A Databricks App at `datacart-storefront` (created but not yet started)
- A Job at `datacart-data-centric :: setup_lakebase_project`

Check the workspace UI to confirm both appear.

## Step 2: Provision the Lakebase Project

```bash
databricks bundle run setup_lakebase_project -t workshop -p fe-vm-ben
```

This runs the setup notebook, which:

1. Calls `WorkspaceClient.database.list_database_instances()` to check whether
   `datacart-data-centric` already exists.
2. If absent, creates it with capacity `CU_1` (small autoscaling default — you can resize later).
3. Polls until the project state is `AVAILABLE`.
4. Writes `/Workspace/Shared/datacart-data-centric/lakebase_project.json` with the project name
   and PG hostname so other labs can read it.

Re-running is safe — the notebook detects the existing project and exits.

## Step 3: Start the Storefront App

```bash
databricks bundle run datacart_storefront -t workshop -p fe-vm-ben
```

Deploys the app's source files and starts it. The app boots, finds it can't reach the database
yet (no Postgres role permissions), and shows "Loading…".

> **Permission to bind the app to Lakebase.** The first time you run an app that declares a
> `postgres` resource (see `app.yaml`), the Databricks Apps platform asks you to confirm which
> Lakebase project to bind to. Pick `datacart-data-centric`. After that, the platform
> auto-injects `PGHOST`, `PGUSER`, `PGPORT`, etc. as env vars on every restart.

## Step 4: Run Labs 1.1 and 2.1

- **Lab 1.1** seeds the `ecommerce` schema and 5 base tables.
- **Lab 2.1** grants the storefront's service principal access to the schema.

After 2.1 the storefront's "Loading…" disappears and you'll see products and a working cart.

## Step 5: Run the Rest of the Labs

The recommended order is the new module map (3.1 → 4.1 → 5.1 → 6.1 → 6.2 → 6.3 → 7.1).

## Re-deploying After Edits

```bash
databricks bundle deploy -t workshop -p fe-vm-ben
databricks bundle run datacart_storefront -t workshop -p fe-vm-ben    # restarts the app
```

`bundle deploy` is incremental — only changed files are uploaded.

## Tearing Down

```bash
databricks bundle destroy -t workshop -p fe-vm-ben
```

This removes the app and the setup job. **It does not delete the Lakebase project** (DABs has
no native destroy hook for it). Delete it manually from Catalog Explorer → Lakebase Postgres →
`datacart-data-centric` → Settings → Delete project.

## Troubleshooting

### "Project `datacart-data-centric` not found" in Lab 1.1

The setup job didn't run, or it ran against a different project name. Verify with:

```bash
databricks postgres list-projects --profile fe-vm-ben | jq '.[].name'
```

Re-run the setup job if needed:

```bash
databricks bundle run setup_lakebase_project -t workshop -p fe-vm-ben
```

### Storefront stays on "Loading…" after Lab 2.1

Look at the app logs:

```bash
databricks apps logs datacart-storefront --profile fe-vm-ben
```

Common causes:
- The app wasn't bound to the Lakebase project (Step 3 confirmation skipped). Re-bind in the
  app's resources tab.
- The SP didn't get USAGE/SELECT/INSERT grants. Re-run Lab 2.1.
- The app started before the schema existed. Re-run `bundle run datacart_storefront` after
  Lab 1.1 finishes.

### `bundle validate` errors about `root_path` in the workshop target

The bundle pins `root_path` to your user's `/Workspace` path. If you're running as a different
user, update `targets.workshop.workspace.root_path` in `databricks.yml` to match yours.

### The federated query in Lab 4.1 errors with "connection refused"

Foreign catalog connections require Lakehouse Federation to be enabled on your SQL warehouse.
Use a serverless SQL warehouse if you don't have classic warehouses configured for federation.

### The Lakehouse Sync in Lab 5.1 doesn't appear in the UI

Lakehouse Sync is gated by region and feature flag — confirm the **Sync to Unity Catalog** option
is visible on your project's page. If not, ask your Databricks contact to enable the feature on
this workspace.
