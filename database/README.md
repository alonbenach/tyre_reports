# Database Area

This folder contains database-specific assets for the SQLite-backed moto application.

## Layout

```text
database/
  README.md
  migrations/
  tools/
  moto_pipeline.db
  moto_pipeline_tmp.db
```

## Boundaries

- `database/migrations/` stores SQL schema migrations.
- `database/tools/` stores migration and DB-operations scripts.
- `src/moto_app/db/` stores runtime DB helpers used by the application.
- `data/` remains reserved for raw source files, archived snapshots, and reference source workbooks.
