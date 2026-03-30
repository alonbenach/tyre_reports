# Database Area

This folder contains database-specific assets for the SQLite-backed moto application.

## Layout

```text
database/
  README.md
  migrations/
  tools/
```

## Boundaries

- `database/migrations/` stores SQL schema migrations.
- `database/tools/` stores migration and DB-operations scripts.
- `src/moto_app/db/` stores runtime DB helpers used by the application.
- `data/` remains reserved for raw source files, archived snapshots, and reference source workbooks.
- runtime SQLite files such as `moto_pipeline.db`, WAL files, and session locks are created here at runtime and are intentionally not versioned in Git.
