# Databricks notebook source
# MAGIC %md
# MAGIC # 🧹 CLEAN UP — PLEASE RUN AT THE END!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Run this notebook once you've finished the workshop.** It tears down the two resources the
# MAGIC workshop provisioned for you, so you don't leave anything running and billing:
# MAGIC
# MAGIC 1. The **DataCart Storefront app** (`storefront-<your-user-id>`)
# MAGIC 2. The **Lakebase Autoscaling project** (`lakebase-workshop-<your-user-id>`) — including **all** of
# MAGIC    its branches, compute endpoints, databases, and data
# MAGIC
# MAGIC <div style="border-left:4px solid #f44336; background:#ffebee; padding:14px 18px; border-radius:4px; margin:16px 0;">
# MAGIC <strong style="color:#c62828;">⚠️ This is destructive and irreversible.</strong>
# MAGIC <div style="color:#333;">Deleting the project removes every branch (production and any dev/PITR
# MAGIC branches), all compute endpoints, all databases, and all data — permanently. Only run this when
# MAGIC you're completely done with the workshop.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Identify the resources to delete
# MAGIC
# MAGIC The project and app names are auto-derived from your numeric Databricks user ID — the same
# MAGIC convention the setup notebook and the labs use. This cell only *looks up* the resources; nothing
# MAGIC is deleted yet.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

w = WorkspaceClient()
me = w.current_user.me()

project_name = f"lakebase-workshop-{me.id}"
app_name = f"storefront-{me.id}"

print(f"User:            {me.user_name}")
print(f"Lakebase project to delete:  {project_name}")
print(f"Storefront app to delete:    {app_name}")

# Report current state (does not delete anything)
project_exists = any(
    p.name == f"projects/{project_name}" for p in w.postgres.list_projects()
)
try:
    w.apps.get(name=app_name)
    app_exists = True
except NotFound:
    app_exists = False

print()
print(f"   Project found? {project_exists}")
print(f"   App found?     {app_exists}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Delete the storefront app
# MAGIC
# MAGIC Removing the app also removes its source deployment and its Lakebase resource binding.

# COMMAND ----------

if app_exists:
    print(f"🗑️  Deleting app '{app_name}'...")
    w.apps.delete(name=app_name)
    print(f"   ✅ App '{app_name}' deleted.")
else:
    print(f"ℹ️  App '{app_name}' not found — nothing to delete (already removed?).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Delete the Lakebase project
# MAGIC
# MAGIC This deletes **all** branches, compute endpoints, databases, and data in the project. It is a
# MAGIC long-running operation — `.wait()` blocks until the delete completes.

# COMMAND ----------

if project_exists:
    print(f"🗑️  Deleting Lakebase project '{project_name}' (all branches, computes, and data)...")
    w.postgres.delete_project(name=f"projects/{project_name}").wait()
    print(f"   ✅ Project '{project_name}' deleted.")
else:
    print(f"ℹ️  Project '{project_name}' not found — nothing to delete (already removed?).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Confirm teardown
# MAGIC
# MAGIC Re-check that both resources are gone.

# COMMAND ----------

project_gone = not any(
    p.name == f"projects/{project_name}" for p in w.postgres.list_projects()
)
try:
    w.apps.get(name=app_name)
    app_gone = False
except NotFound:
    app_gone = True

print("=" * 60)
print(f"   Project '{project_name}' deleted?  {project_gone}")
print(f"   App '{app_name}' deleted?          {app_gone}")
print("=" * 60)

if project_gone and app_gone:
    print("🎉 Cleanup complete — all workshop resources have been removed. Thanks for participating!")
else:
    print("⚠️  Some resources still appear to exist. Re-run this notebook, or check the Lakebase / Apps UI.")

