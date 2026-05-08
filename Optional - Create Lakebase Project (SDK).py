# Databricks notebook source
# MAGIC %md
# MAGIC # Optional: Create the Lakebase Project via SDK (No DABs)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## When to use this notebook
# MAGIC
# MAGIC The default workshop path uses Databricks Asset Bundles (DABs) to provision the Lakebase
# MAGIC Autoscaling project — see `WORKSHOP_SETUP.md` and `databricks-storefront/databricks.yml`.
# MAGIC `bundle deploy` creates the project, the storefront app, and the binding between them in
# MAGIC one shot. **That's the recommended path.**
# MAGIC
# MAGIC Use this notebook **only if** you can't or don't want to deploy with DABs — for example:
# MAGIC
# MAGIC - Your workspace doesn't have the Workspace Files / serverless compute features required by the workspace-UI deploy
# MAGIC - You don't have the Databricks CLI installed and don't want to install it
# MAGIC - You want to step through the SDK calls yourself to learn the underlying API
# MAGIC
# MAGIC This notebook **only creates the Lakebase project**. To get the storefront app running you'll
# MAGIC still need to deploy it manually — see the **"Deploy the storefront app manually"** section
# MAGIC at the bottom for the UI steps.
# MAGIC
# MAGIC > **Naming compatibility.** The project this notebook creates uses the exact same name pattern
# MAGIC > as the DABs version (`lakebase-workshop-<your-user-id>`), so all the rest of the workshop
# MAGIC > labs (1.1 onward) will discover it automatically with no further configuration.
# MAGIC
# MAGIC > **Docs**: [Lakebase Autoscaling Projects](https://docs.databricks.com/aws/en/oltp/projects/) | [Manage with bundles](https://docs.databricks.com/aws/en/oltp/projects/manage-with-bundles)

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration
# MAGIC
# MAGIC The project ID is auto-derived from your numeric Databricks user ID — same convention as
# MAGIC the bundle. The display name combines your first and last name for readability in the UI.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me()

project_name = f"lakebase-workshop-{me.id}"
display_name = f"Lakebase Workshop — {me.name.given_name} {me.name.family_name}"

# Compute settings — match the DABs config in resources/lakebase_instance.yml
PG_VERSION = 17
MIN_CU = 0.5
MAX_CU = 2.0
SUSPEND_TIMEOUT_SECONDS = 300

print(f"User:                {me.user_name}")
print(f"Project ID:          {project_name}")
print(f"Display name:        {display_name}")
print(f"Postgres version:    {PG_VERSION}")
print(f"Compute:             {MIN_CU} – {MAX_CU} CU, scale-to-zero after {SUSPEND_TIMEOUT_SECONDS}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Project
# MAGIC
# MAGIC `create_project` is a long-running operation — `.wait()` blocks until the project is in
# MAGIC `AVAILABLE` state. Idempotent: re-running this cell against an existing project returns
# MAGIC without recreating it.

# COMMAND ----------

from databricks.sdk.service.postgres import (
    Project, ProjectSpec, ProjectDefaultEndpointSettings, Duration
)

# Re-run safety: skip create if project already exists.
existing = next(
    (p for p in w.postgres.list_projects() if p.name == f"projects/{project_name}"),
    None,
)

if existing:
    print(f"ℹ️  Project '{project_name}' already exists — skipping create.")
    project_obj = existing
else:
    print(f"🔄 Creating project '{project_name}'...")
    project_obj = w.postgres.create_project(
        project=Project(spec=ProjectSpec(
            display_name=display_name,
            pg_version=PG_VERSION,
            default_endpoint_settings=ProjectDefaultEndpointSettings(
                autoscaling_limit_min_cu=MIN_CU,
                autoscaling_limit_max_cu=MAX_CU,
                suspend_timeout_duration=Duration(seconds=SUSPEND_TIMEOUT_SECONDS),
            ),
        )),
        project_id=project_name,
    ).wait()
    print(f"✅ Project '{project_name}' created!")

workspace_host = w.config.host.rstrip("/")
print(f"\n🔗 Lakebase UI: {workspace_host}/lakebase/projects/{project_obj.uid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify the Production Branch & Endpoint

# COMMAND ----------

import time

# The default 'production' branch is created automatically.
branches = list(w.postgres.list_branches(parent=f"projects/{project_name}"))
prod_branch = next(b for b in branches if b.status and b.status.default)
print(f"✅ Production branch: {prod_branch.name}")

# Wait for the primary compute endpoint to be ready (typically <60s).
endpoints = list(w.postgres.list_endpoints(parent=prod_branch.name))
for i in range(30):
    if endpoints:
        break
    time.sleep(10)
    endpoints = list(w.postgres.list_endpoints(parent=prod_branch.name))
    print(f"   waiting for endpoint... ({(i+1)*10}s)")

if not endpoints:
    raise RuntimeError("Compute endpoint not available after 5 minutes.")

ep = endpoints[0]
print(f"\n✅ Endpoint ready:")
print(f"   Name: {ep.name}")
print(f"   Host: {ep.status.hosts.host}")
print(f"   Database: databricks_postgres")

# COMMAND ----------

# MAGIC %md
# MAGIC ## You're done with the SDK setup
# MAGIC
# MAGIC The Lakebase project is up and running. The remaining workshop labs (Lab 1.1 onward) will
# MAGIC find this project by name automatically and continue from there.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Deploy the storefront app manually
# MAGIC
# MAGIC The DAB bundle would normally deploy the storefront app and bind it to the Lakebase project
# MAGIC for you. Without DABs, do this in the UI:
# MAGIC
# MAGIC ### A. Create the app
# MAGIC
# MAGIC 1. Open **Compute → Apps** in the workspace.
# MAGIC 2. Click **Create app** → **Custom**.
# MAGIC 3. Name it `datacart-storefront` and click **Create**.
# MAGIC
# MAGIC ### B. Add the Lakebase project as an app resource
# MAGIC
# MAGIC 1. On the app's page, go to **Settings → Resources**.
# MAGIC 2. Click **Add resource** → **Database**.
# MAGIC 3. Pick your project (the one this notebook just created), production branch,
# MAGIC    `databricks_postgres` database. Permission: **CAN_CONNECT_AND_CREATE**.
# MAGIC 4. Save. The platform auto-injects `PGHOST`, `PGUSER`, `PGPORT`, `PGDATABASE` env vars
# MAGIC    on the next app deploy.
# MAGIC
# MAGIC ### C. Deploy the source code
# MAGIC
# MAGIC 1. Click **Deploy** on the app's page.
# MAGIC 2. Set the source path to wherever you've placed the `datacart-storefront/` source files
# MAGIC    in your workspace (e.g. `/Workspace/Users/<your-email>/lakebase-in-a-box-workshop-data-centric/datacart-storefront`).
# MAGIC 3. Click Deploy. The app starts and shows "Loading…" until you grant the SP database access in Lab 2.1.
# MAGIC
# MAGIC From here, continue the workshop with **Lab 1.1**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)
# MAGIC
# MAGIC If you ever want to tear down what this notebook created (without `databricks bundle destroy`):

# COMMAND ----------

# Uncomment to delete the project. WARNING: deletes all branches, computes, databases, data.
# w.postgres.delete_project(name=f"projects/{project_name}").wait()
# print(f"🗑️  Deleted project {project_name}")

