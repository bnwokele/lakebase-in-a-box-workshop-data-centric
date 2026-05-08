# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Lakebase Autoscaling Project
# MAGIC
# MAGIC One-time bundle setup task. Idempotently creates the Lakebase Autoscaling project that
# MAGIC the data-centric workshop labs target. Safe to re-run — if the project already exists it
# MAGIC is detected and reused.
# MAGIC
# MAGIC **Run via:**
# MAGIC ```bash
# MAGIC databricks bundle run setup_lakebase_project -t workshop -p fe-vm-ben
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("lakebase_project", "datacart-data-centric")
dbutils.widgets.text("pg_version", "17")

lakebase_project = dbutils.widgets.get("lakebase_project")
pg_version = dbutils.widgets.get("pg_version")

print(f"Target project: {lakebase_project}")
print(f"Postgres version: {pg_version}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find or create the project
# MAGIC
# MAGIC Lakebase Autoscaling projects are exposed through the `database` API as instances with
# MAGIC capacity policy `AUTOSCALE`. We list existing instances and check for a name match before
# MAGIC creating a new one.

# COMMAND ----------

existing = None
for instance in w.database.list_database_instances():
    if instance.name == lakebase_project:
        existing = instance
        break

if existing:
    print(f"Project '{lakebase_project}' already exists — reusing.")
    instance = existing
else:
    print(f"Creating project '{lakebase_project}'...")
    instance = w.database.create_database_instance(
        database_instance=DatabaseInstance(
            name=lakebase_project,
            capacity="CU_1",
        )
    )
    print(f"Created. Waiting for the project to become AVAILABLE...")
    # Poll until the instance is ready
    import time
    for _ in range(60):
        instance = w.database.get_database_instance(name=lakebase_project)
        state = getattr(instance, "state", None)
        print(f"  state={state}")
        if state and str(state).endswith("AVAILABLE"):
            break
        time.sleep(10)

print()
print(f"Project name: {instance.name}")
print(f"Hostname:     {getattr(instance, 'read_write_dns', '<not yet assigned>')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Surface the connection details for the labs
# MAGIC
# MAGIC The labs read these from the job output. They are also written to a workspace file so any
# MAGIC notebook in this workshop can import them.

# COMMAND ----------

import json

connection_details = {
    "lakebase_project": instance.name,
    "endpoint_name": f"projects/{instance.name}/branches/production/endpoints/primary",
    "pghost": getattr(instance, "read_write_dns", None),
}
print(json.dumps(connection_details, indent=2))

# Persist a JSON pointer file at a stable workspace path so the labs can find it.
import os
output_path = "/Workspace/Shared/datacart-data-centric/lakebase_project.json"
os.makedirs(os.path.dirname(output_path), exist_ok=True)
with open(output_path, "w") as f:
    json.dump(connection_details, f, indent=2)
print(f"\nWrote {output_path}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps(connection_details))

