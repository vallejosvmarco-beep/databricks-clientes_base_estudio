# clientes_base

Bundle `clientes_base`: volumen de landing en Unity Catalog y esquemas medallion (`clientes_base`, `clientes_base_bronze`, `clientes_base_silver`, `clientes_base_gold`).

## Important

Free Edition capabilities and quotas vary. This bundle uses the **existing** catalog `datosami` and assumes permissions to create schemas and managed volumes. It does not create a catalog, compute, or warehouse. Databricks Asset Bundles deployment requires Databricks CLI authentication. A GitHub repository alone does not automatically deploy resources.

## Recommended first-time setup (Free Edition)

1. Create a GitHub repository using this folder as a template.
2. Open the repository in a Databricks Git folder, or import the `src/initialize` notebook into your workspace.
3. Check `CATALOG` (`datosami`) and `PROJECT` (`clientes_base`) in the notebook, then run it using available compute.
4. Upload source files to `/Volumes/datosami/clientes_base/landing/`.
5. Ask your available Databricks AI coding assistant to create the ingestion pipeline and Delta tables. Genie is generally for querying data, not provisioning pipelines.

## Bundle CLI path (where supported)

The bundle includes the SQL initialization job `clientes_base_initialize`; its SQL uses fixed `datosami.clientes_base*` names. Edit `src/initialize.sql` if you change the catalog or schema variables. Set a valid existing SQL warehouse ID and authenticate the CLI:

```bash
databricks bundle validate --var warehouse_id=<SQL_WAREHOUSE_ID>
databricks bundle deploy --var warehouse_id=<SQL_WAREHOUSE_ID>
databricks bundle run clientes_base_initialize --var warehouse_id=<SQL_WAREHOUSE_ID>
```

`bundle deploy` deploys the job definition; `bundle run` executes initialization. The CLI route depends on an available SQL warehouse and permissions; the notebook route is the recommended fallback.

## Suggested AI prompt

Read CSV/Parquet files in `/Volumes/datosami/clientes_base/landing/`, infer schemas, create append-safe bronze Delta tables in `datosami.clientes_base_bronze`, track source filename and ingestion timestamp, quarantine malformed rows, and explain the generated pipeline before execution. Never delete landing files without approval.

## Layout

- `databricks.yml`: bundle settings
- `resources/init.yml`: SQL initialization job `clientes_base_initialize`
- `src/initialize`: recommended Free Edition initialization notebook
- `src/initialize.sql`: example SQL initialization script
