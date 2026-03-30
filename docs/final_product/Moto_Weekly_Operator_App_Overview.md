# Moto Weekly Operator App Overview

## Why This Document Exists

This document is meant to be a complete, human-readable overview of the Moto Weekly Operator App.

It is written for a person reading alone, without access to an assistant, without browsing for extra context, and without needing to jump back and forth between many small technical documents.

The goal is to make it possible to sit down with this one document and understand:

- what the app is
- why it exists
- what problems it solves
- how the major parts work
- how the parts interact with each other
- what is already finished
- what is still planned
- what questions are still worth thinking about before packaging and rollout

This document is intentionally longer than the README and less technical than the module specs. It tries to explain the system in plain language while still being accurate enough to support real design review.

## Current Status Snapshot

At the time of this update, the app is no longer just a design or prototype:

- the SQLite-backed backend is implemented
- the PySide6 desktop UI is implemented
- CI is active with lint and test gates
- a portable packaged build now launches successfully
- local packaged runs complete end to end
- the latest trusted weekly output has been checked against the packaged app and matched

That means the project has moved from "can this be built?" into "what still needs hardening before broader rollout and long-term maintenance?"

## The Short Version

The Moto Weekly Operator App is a Windows desktop tool for running a weekly motorcycle reporting process in a safe and repeatable way.

Instead of depending on someone remembering a sequence of scripts, spreadsheets, folders, and special cases, the app gives the process a clear structure:

- the weekly input file is staged into the app
- the data is loaded into a local SQLite database
- processed datasets are built from that database
- Excel and PDF reports are generated
- the run is logged
- the operator can see the results and the system status in one place

The app is specifically meant to reduce operational fragility. In other words, it is not only about producing the reports. It is also about making sure the process can still run if the main technical owner is unavailable.

## What Problem the App Is Solving

Before this app, the reporting process had several common risks:

- too much practical knowledge lived in one person’s head
- there was too much room for manual mistakes
- important working files could be changed, moved, or deleted outside the process
- rerunning a week was not always safe or obvious
- there was no central operator-facing interface

These are the kinds of problems that usually do not show up when the pipeline is first built. They show up later, when:

- someone needs to rerun old data
- someone accidentally overwrites a file
- someone else has to operate the process
- outputs are needed on a deadline
- the owner is sick, busy, or on vacation

So the app is not just a nicer wrapper around the old reports. It is a structural improvement to how the reporting process is stored, run, and supervised.

## Main Product Goal

The main goal of the app is simple:

Make the weekly motorcycle reporting process safe enough and clear enough that a non-technical person can run it reliably.

That broad goal breaks down into a few smaller ones:

1. Keep the input process simple.
2. Keep the working data under system control.
3. Preserve traceability of what was run and when.
4. Keep report generation consistent.
5. Prevent accidental duplication or silent data corruption.
6. Make the current system state visible from the UI.
7. Build a stable base for packaging and shared-drive use later.

## Current Scope

The app is currently focused on motorcycle data only.

That means:

- the input CSV may contain more categories, but the processing scope is motorcycle rows
- the reporting logic is currently built around the motorcycle use case
- the UI, storage model, and workflow are intentionally narrow so the first productionized version stays manageable

This limited scope is a strength, not a weakness. It keeps the app focused on the real business need today while leaving room to expand later if the tool proves useful.

## What the App Produces

At the moment, the app produces two report families:

- `Price Positioning`
- `Offeror Focus`

Each report family can generate:

- an Excel file
- a PDF file

The report files are stored in a structured folder layout:

```text
reports/
  price_positioning/
    excel/
    reports/
  offeror_focus/
    excel/
    reports/
```

This is deliberate. It keeps file types separated and makes it easier for an operator to understand where current outputs live.

## Core Design Decisions

There are a few important design choices that shape the whole app.

### 1. CSV is still the input format

The source system still exports a CSV file, so the app still accepts CSV as its input.

That part did not need to change.

### 2. SQLite is the system of record for processed data

The big change is what happens after the CSV is received.

Instead of treating Excel or ad hoc working files as the important source of truth, the app stores processed state in SQLite. SQLite is a small file-based SQL database. It does not require a separate database server, which makes it practical for this kind of internal tool.

This means the app has a central controlled place for:

- imported snapshot metadata
- reference data
- staged source rows
- transformed data
- report-ready data
- run history
- output metadata

### 3. Excel and PDF are outputs, not dependencies

This is a crucial principle.

Excel and PDF are good formats for people to read, share, and present.

They are not good formats for the app’s internal working state.

So the app treats them as final deliverables, not as something later pipeline steps depend on.

### 4. Runs and business data are not the same thing

Every app execution gets a `run_id`, but that does not mean the business data should duplicate.

This distinction matters:

- `run_id` is for audit and logging
- `snapshot_date` and business keys are for data uniqueness

That is why rerunning a week replaces that week’s snapshot rather than silently appending a second copy of the same business data.

### 5. The UI should guide operations, not contain business logic

The desktop interface is there to help the operator.

It should:

- collect input
- show state
- block risky actions
- show logs and outputs

It should not:

- own transformation logic
- write directly to the database on its own
- become a second copy of backend rules

That separation is important for long-term maintainability.

## Architecture Overview

The app is organized into three broad layers.

### Frontend Layer

This is the part the operator sees and interacts with.

Right now that is the PySide6 desktop UI.

Its responsibilities are things like:

- choosing a weekly CSV
- staging it into the app
- selecting which staged snapshot to run
- showing run history
- opening output files
- showing logs and status summaries

The frontend should help the user make safe choices, but it should not own core business rules.

### Backend/Application Layer

This is the orchestration layer.

When the user starts a run, the backend is the part that decides what to do and in what order.

It handles things like:

- loading configuration
- checking runtime paths
- refreshing reference data if requested
- ingesting the staged input
- building processed datasets
- generating exports
- updating run logs and run status

If we describe the app as a body, the backend is the nervous system that coordinates the work.

### Data/Database Layer

This is the persistence layer.

Its role is to store and retrieve structured data safely.

It includes:

- the SQLite database file
- SQL schema
- migrations
- connection helpers
- query helpers

This layer is where the app’s long-term memory lives.

## Important Terms Explained in Plain Language

This section defines a few terms that appear throughout the design.

### Snapshot

A snapshot is one weekly input state.

In practice, a snapshot is identified by a date such as `2026-03-17`.

The app treats each weekly CSV as one snapshot of the business data at that moment in time.

### Stage / Staging

The stage layer is the first structured database layer after raw input.

Its purpose is:

- validate the input
- load it into SQL
- preserve source-level rows in a controlled format

It is not yet the fully cleaned analytical dataset.

### Silver Layer

The silver layer is the cleaned and enriched working dataset.

This is where the source data becomes analytically usable.

Typical operations here include:

- filtering to motorcycle rows
- normalizing values
- matching source rows to canonical reference data
- cleaning and deduplicating data
- deriving fields used by reporting logic

### Gold Layer

The gold layer is the report-ready layer.

These are the datasets that exist specifically to support reporting and analysis.

They are more aggregated and more business-facing than the silver layer.

### Mart

A mart, short for “data mart,” is a structured report-oriented dataset.

When this document refers to reporting marts, it means the final SQL datasets built for reporting purposes, such as:

- brand-level weekly summaries
- seller-level summaries
- price positioning summaries
- recap tables

You can think of a mart as a table shaped specifically for a report or a report section.

### Reference Data

Reference data is supporting business data that helps interpret the weekly source file.

Examples include:

- canonical fitment mappings
- price lists
- campaign discount tables

The weekly CSV alone is not enough to produce the final reports correctly. It needs these supporting business rules and lookup tables.

### Migration

A migration is a controlled database change.

Instead of changing the database structure manually, the app keeps ordered SQL migration files so the database schema can evolve in a repeatable way.

### Run Record

A run record is a stored history entry for a single app execution.

It captures things like:

- when the run started
- when it finished
- whether it succeeded
- which snapshot it targeted
- what source file was used
- what error occurred if it failed

## Main Functional Modules

The app has been planned and built in bounded modules. Below is what each module means in practical terms.

### 1. App Shell

The app shell is the coordinator.

This is the part that turns separate services into one weekly process.

When a run starts, the app shell is what says:

- first do configuration
- then create a run record
- then ingest
- then transform
- then build marts
- then export
- then mark the run finished

It is the glue that ties the app together.

### 2. Configuration

The configuration module defines where things live and what defaults are used.

This includes paths for:

- intake files
- raw archive
- database
- reports
- logs
- reference sources

It also controls how the app behaves differently in development and production later on.

### 3. Storage and Migrations

This is the database foundation.

It defines:

- tables
- views
- metadata storage
- migration runner behavior

Without this module, the rest of the app would not have a stable storage layer.

### 4. Reference Data

This module loads and refreshes the business-maintained lookup data the reports need.

At the moment, some of that source material still begins life as spreadsheets, but the app loads it into SQL so runtime logic reads from the database rather than from spreadsheets directly.

This reduces fragility and makes the pipeline easier to reason about.

### 5. Ingestion

The ingestion module handles the weekly CSV on arrival.

It is responsible for:

- making sure the file is really a CSV
- checking required columns
- assigning it to the intended snapshot date
- staging it into the intake area
- archiving it into raw storage
- loading source rows into SQL
- recording provenance information

This module is very important operationally, because bad input handling is one of the fastest ways to make a tool frustrating or unsafe.

### 6. Transformation

The transformation module turns staged source rows into usable analytical rows.

This is where the main data-cleaning and enrichment work happens.

That includes:

- filtering to the relevant product category
- matching source rows to canonical reference records
- deriving business fields used later in the reports
- producing the silver dataset

### 7. Reporting Marts

This module builds the gold layer, meaning the report-ready datasets.

Instead of making report builders compute everything from scratch every time, the marts provide structured and reusable reporting tables.

That makes the reporting logic clearer and easier to validate.

### 8. Exports

The exports module turns report-ready SQL data into management-facing files.

It currently produces:

- Price Positioning Excel/PDF
- Offeror Focus Excel/PDF

It also stores output metadata and keeps the report folder layout tidy.

### 9. Operator UI

This is the desktop application itself.

It provides the human interface for:

- staging files
- running snapshots
- checking results
- opening outputs
- reading instructions

### 10. Observability and Run Control

This module makes the app supportable.

That means it handles:

- run tracking
- logs
- operator-friendly error messages
- historical run listings
- high-level DB coverage summaries

It exists so the app can answer questions like:

- What happened during the last run?
- Which week was loaded?
- Why did the run fail?
- Which weeks exist in the database?

### 11. Packaging and Distribution

This is the module that will define how the app is turned into a packaged internal tool.

It is not the runtime logic itself. It is the set of rules and assets for distribution, installation, and updates.

### 12. Testing and Parity

This module exists to prove that the new app still behaves like the old reporting logic where it should.

“Parity” means the new system should match the old system closely enough on important outputs and metrics.

It also protects the codebase against regressions when future changes are made.

### 13. Access Control and Admin Mode

This is a planned operational module.

It is about safe usage on a shared drive.

Its job will be to define:

- how only one user can hold the writable session at a time
- how other users can still open the app in read-only mode
- what actions are admin-only
- how stale locks are recovered

### 14. Environments and CI/CD

This is another planned operational module.

It is about how the app is developed and released safely over time.

It will define:

- what “development” means
- what “production” means
- how tests are enforced
- how merges to `main` are protected
- how packaging/release steps happen

CI/CD stands for Continuous Integration / Continuous Delivery. In plain language, this means automated rules that check the code and reduce the chance of bad changes reaching the main branch or production package.

## How the Weekly Workflow Works

The weekly operator flow is intentionally split into two separate parts.

This split was added because earlier versions of the UI made it too easy to confuse:

- which date was being used for staging
- which staged file was actually being run

### Part 1. Stage a Weekly CSV

The operator does this first.

The steps are:

1. Drop or browse the weekly CSV.
2. Choose the snapshot date that should be assigned to it.
3. Stage the file into the app intake folder under that chosen name.

This creates a controlled staged file such as:

`data/ingest/2026-03-17.csv`

This step is about preparing the input safely.

It does not yet run the pipeline.

### Part 2. Run a Staged Snapshot

The operator then chooses which staged snapshot to execute.

This is separate from the date picker.

That separation is important because it prevents the operator from accidentally changing the run target just by adjusting the staging date.

The operator then chooses run options such as:

- whether to generate PDFs
- whether to replace an already existing snapshot
- whether to refresh reference data first

Then the run starts.

## What Happens During a Run

When the operator starts a weekly run, the app performs a controlled sequence of steps.

### 1. Create a Run Record

Before doing data-changing work, the app stores a run entry in the database.

This gives the process an audit trail from the very beginning.

### 2. Optionally Refresh Reference Data

If requested, the app refreshes SQL-backed reference tables from the maintained spreadsheet sources.

### 3. Ingest the Chosen Snapshot

The staged CSV is validated and loaded into the database stage layer.

### 4. Build the Silver Layer

The app transforms the staged rows into cleaned and enriched rows that the reporting logic can use.

### 5. Build the Gold Reporting Marts

The app rebuilds the report-ready datasets for use by the exports.

### 6. Generate Report Files

The app generates Excel files and, if selected, PDF files.

### 7. Record Output Metadata

The system records what files were generated and where they live.

### 8. Mark the Run as Succeeded or Failed

The final run status is stored in the database and reflected in the log.

## Data Flow, End to End

The data flow can be summarized like this:

`Dropped CSV -> intake -> raw archive -> SQL stage -> SQL silver -> SQL gold marts -> Excel/PDF reports`

At the same time, operational metadata flows like this:

`run start -> source import metadata -> generated output metadata -> logs -> final run status`

This separation between business data and run metadata is one of the reasons the app is much safer than a loose collection of scripts.

## Current User Interface

The current desktop app contains five main screens.

### Home

The Home screen is the operational summary.

It shows:

- last run status
- recent run count
- current outputs count
- a coverage summary of loaded database weeks
- a latest-log summary block
- detail fields such as latest snapshot and paths

The Home screen is meant to answer the question:

“Is the system healthy, and are we up to date?”

### Weekly Run

This is the main operational screen.

It allows the user to:

- stage a CSV
- assign the staging date
- see the staged file path
- select which staged snapshot to run
- choose PDF on/off
- choose replace mode
- choose reference refresh
- follow the live log

### Run History

This shows the recent run records stored in the database.

It is more about process history than current output state.

### Outputs

This screen shows the current live files on disk for the selected report family.

This is important:

It does not show every historical generation event.

That history still exists in the database, but the UI view is intentionally simplified to show the actual currently existing files.

### Instructions

This is the built-in operator help area.

It exists so the app itself can carry some operational guidance instead of depending on memory.

## Output Structure

The report output folders are currently organized like this:

```text
reports/
  price_positioning/
    excel/
      PRICE_POSITIONING_Wxx_Poland.xlsx
    reports/
      PRICE_POSITIONING_Wxx_Poland.pdf
  offeror_focus/
    excel/
      offeror_focus_Wxx_Poland.xlsx
    reports/
      offeror_focus_Wxx_Poland.pdf
```

This structure supports two useful goals:

1. It is easy for a human to browse.
2. It matches the app’s report-family view in the Outputs tab.

## Logging, Diagnostics, and Audit Trail

Every run produces three kinds of traceability.

### 1. Run Record in SQLite

This stores:

- run id
- snapshot
- start time
- end time
- success/failure
- source file information
- error summary if needed

### 2. Log File on Disk

This stores the detailed technical execution log for the run.

This is the place to look when the UI tells you something failed and you want the detailed reason.

### 3. Generated Output Metadata

This stores which outputs were generated, when, and for which snapshot.

The UI no longer shows this full history directly in the outputs tab, but the metadata still exists for audit purposes.

## Safety Features Already Implemented

Several important safeguards are already in place.

### Duplicate Snapshot Protection

If the selected snapshot already exists and replace mode is off, the app warns the user before the run starts.

### Explicit Stage vs Run Separation

Staging and running are separate actions, reducing the chance of accidental wrong-date runs.

### Snapshot-Scoped Exports

Exports now run against the chosen run snapshot, not simply the maximum snapshot in the database.

This matters because it prevents a mistaken future-dated snapshot from hijacking current report labels.

### Live Outputs View

The Outputs tab shows what currently exists on disk, not every historical write event.

### Home Coverage Status

The home screen can now show how far the database is populated by year and week.

### Operator-Friendly Errors

Not every error is perfect yet, but the app already tries to present plain-English messages where possible instead of only raw tracebacks.

## Current Database Coverage Example

At the time of the latest check, the database coverage showed:

- year `2026`
- loaded through week `12`
- weeks present: `07, 08, 09, 10, 11, 12`

This is exactly the kind of quick status signal the home screen is meant to provide.

## What Still Needs Attention Before Packaging

The app is already solid enough to use and review seriously. Still, a few operational areas should be ironed out before packaging and shared-drive rollout.

### 1. Single-User Writable Access

This is one of the most important remaining items.

The intended behavior is:

- one user gets the writable/operator session
- additional users can still open the app, but only in read-only mode
- read-only users should still be able to open outputs and inspect status

This should prevent two people from trying to operate the shared app at the same time.

### 2. Admin Mode

An admin mode is needed for ongoing support and controlled cleanup.

Likely admin tasks include:

- refreshing references
- removing a mistaken staged CSV
- rebuilding a selected snapshot
- recovering from stale locks
- possibly other sensitive maintenance actions

### 3. Dev vs Prod Runtime Model

The project now needs a more formal distinction between:

- development mode
- production/shared-drive mode

This matters for:

- path behavior
- packaging expectations
- testing
- safe ongoing development after rollout

### 4. CI/CD and Merge Gates

From this point on, the project should move toward:

- automated test execution
- merge protection
- CI checks before changes reach `main`

This is especially important now that the app is turning into a real internal operational tool.

## Shared-Drive Direction

The intended production model is not a server-hosted web app.

It is a packaged desktop app distributed from a shared drive.

That means the executable and its nearby runtime components should stay close together for convenience.

In plain terms, that means the production structure should keep related parts near each other, such as:

- the packaged app
- the database
- logs
- intake files
- reports

This is what was meant earlier by “sort of local” even though the location is shared. The goal is convenience and predictability, not physical isolation.

## What the App Has Already Become

It is worth noticing how far the project has already moved.

This is no longer just:

- a script
- a set of report notebooks
- a manually run folder process

It is now much closer to:

- an internal operations application
- a controlled reporting workflow
- a supportable system with logs and state
- a tool that can realistically be handed to another operator

That is a meaningful shift.

## Questions Worth Asking While Reviewing This

If you want to use this document as a quiet design review on a long ride, these are good questions to ask yourself:

### Operator Workflow

- Is the weekly process now simple enough?
- Are there any steps that still feel too “technical”?
- Is the stage-then-run model the right balance between safety and convenience?

### Data Integrity

- Are there any ways the wrong week could still be loaded accidentally?
- Should more warnings be added for strange dates or large date gaps?
- Are there any remaining places where editable files still matter too much?

### Report Structure

- Are the two report families organized clearly enough?
- Should outputs show more or less detail?
- Is the distinction between current files and historical generation events right?

### Operational Safety

- Which actions should become admin-only?
- What would a backup operator still find confusing?
- What would happen if the app is opened by two people at once?

### Engineering Future

- Is SQLite still the right scope fit?
- Is the shared-drive model still the most practical path?
- What should be enforced in tests before future merges?

## Related Documents

If you want to go deeper after reading this overview, the most relevant supporting documents are:

- [`README.md`](/c:/Users/benacal001/Documents/projects/moto_analysis/docs/final_product/README.md)
- [`01_app_shell.md`](/c:/Users/benacal001/Documents/projects/moto_analysis/docs/final_product/modules/01_app_shell.md)
- [`08_exports.md`](/c:/Users/benacal001/Documents/projects/moto_analysis/docs/final_product/modules/08_exports.md)
- [`09_operator_ui.md`](/c:/Users/benacal001/Documents/projects/moto_analysis/docs/final_product/modules/09_operator_ui.md)
- [`10_observability_and_run_control.md`](/c:/Users/benacal001/Documents/projects/moto_analysis/docs/final_product/modules/10_observability_and_run_control.md)
- [`11_packaging_and_distribution.md`](/c:/Users/benacal001/Documents/projects/moto_analysis/docs/final_product/modules/11_packaging_and_distribution.md)
- [`13_access_control_and_admin_mode.md`](/c:/Users/benacal001/Documents/projects/moto_analysis/docs/final_product/modules/13_access_control_and_admin_mode.md)
- [`14_environments_and_cicd.md`](/c:/Users/benacal001/Documents/projects/moto_analysis/docs/final_product/modules/14_environments_and_cicd.md)

## Final Note

The main thing this app is trying to do is not just “generate a report.”

It is trying to make a weekly reporting responsibility dependable.

That means the real product is not only the PDFs and Excel files. The real product is a safer process:

- a process that stores its state properly
- a process that can be rerun intentionally
- a process that leaves evidence of what happened
- a process that another person can operate with confidence

That is the larger idea holding the whole project together.
