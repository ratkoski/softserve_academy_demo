# Write-up — design decisions, trade-offs and AI-assisted development

## 1. Why this dataset

The course labs used synthetic transactions. For the final project I chose a real
source which is OpenSky Network's ADS-B feed because it provides naturally what the
requirements ask for: a genuine event stream, reference data that changes over time,
and data quality problems that are real rather than injected. The patterns are the
ones practised in the labs: Auto Loader, Lakeflow, SCD2, bundles. Applied to a
domain where the mistakes are not scripted. Scope was limited deliberately (one
bounding box, no flight-level aggregation, Genie instead of a custom RAG) 
Every cut is listed under Known limitations.

## 2. Design decisions

**One pipeline, two sources, switched by configuration.** The positions table reads
Auto Loader on dev (Free Edition has no Event Hubs) and the Kafka source on prod.
The branch is selected by a `positions_source` key that the bundle sets per target.
The alternative two pipelines or two files would have doubled the code and
broken the "same code everywhere" principle that makes CI/CD meaningful.

**Quarantine instead of drop.** Lakeflow expectations count violations but discard
the rows. I wanted the rows. A flagged view computes `rejection_reasons` as an array
and feeds two tables: silver gets rows with an empty array, quarantine gets the rest
with every reason that applied. The rules live in `src/transformations.py`, shared
between the pipeline and the unit tests, so the tested code is the deployed code.

**SCD Type 2 declaratively.** In the team project I wrote a two-pass MERGE by hand.
Here `create_auto_cdc_flow` with `stored_as_scd_type=2` replaces it. The one subtlety
was the initial load: the first version of every aircraft was valid only from the
moment the registry was first loaded, so positions observed earlier matched nothing.
The fix opens the first version from 1900-01-01. It is unit-tested.

**Per-version surrogate keys and an as-of join.** `aircraft_key` hashes `icao24`
together with `__START_AT`, so each historical version of an aircraft has its own
key. The fact resolves its key by joining on the version valid at `snapshot_ts`.
Aircraft missing from the registry (about 35%) resolve to a sentinel row `-1`
rather than being dropped a map with "Unknown" is more honest than a map with gaps.

**Incremental fact via Change Data Feed.** Rebuilding `fact_positions` in full is
fine at 40k rows and wrong at 40M. CDF on `silver_positions` plus a watermark table
lets the gold job read only inserts since the last processed version and MERGE them
on the natural key. Re-running without new data is a no-op, this is the projects
advanced capability and its idempotency proof in one.

**Governance as a job task.** Row filters and column masks are applied by a third
task after every gold build. Two reasons: `overwrite` can drop table level policies,
and policies in code are policies under version control. Access itself is
table driven (`operator_access`, `privileged_users`) so granting access is an
`UPDATE`, not a deploy and no emails are committed.

**Producer outside the platform.** It is a Python process authenticating with OAuth2
and refreshing its token, on prod it writes to Event Hubs, on dev to JSON files.
Keeping it outside Databricks mirrors a real IoT feed and keeps the consumer honest.

## 3. Cost and performance trade-offs

**Serverless everywhere.** Convenient and the only option on both workspaces, but
with no hard budget cap. The pipeline runs triggered, not continuous: a 30 minute
cadence costs minutes of compute per hour instead of an always on cluster. Continuous
mode would only be justified if seconds of latency mattered to a consumer.

**Event Hubs retention of one hour** (set at namespace level) forces the job to run
at least twice an hour. With 24 hour retention the cadence could be hourly and the
producer could be restarted freely. The constraint is documented, not hidden.

**Full rebuild of dimensions and aggregates.** Dimensions are small and aggregates
are cheap at this volume, only the fact is incremental. At ten times the data the
aggregates would move to incremental materialized views in the pipeline.

**Small compute by design.** Nothing in this platform needs more than a small node:
half a million registry rows and tens of thousands of positions per day. Right sizing
was a lesson from the course, and it is cheaper to scale up when needed than to pay
for headroom that is never used.

**Environment constraints.** The workspace originally designated as prod could not
run a Lakeflow pipeline: serverless is disabled by a cost policy, cluster creation
requires a policy  and both available policies allow only all-purpose clusters. Prod
therefore runs on a Premium tier trial workspace sharing the same metastore and
catalog, with full CI. This is documented rather than worked around: knowing which
compute a platform is allowed to use is part of designing it.

## 4. Where AI assisted development helped and the guardrails

I used an AI assistant throughout, in a specific way: as a pair programmer that
drafts, explains and reviews, while I decide, run and verify.

**Where it helped most.** Translating known patterns into this domain quickly: the
quarantine view, the SCD2 as-of join, the CDF watermark loop. Debugging error
messages I had not seen before (`COST_CONTROL_ENTITLEMENT_DENIED`, Kafka SASL format
errors, bundle state pointing at a deleted pipeline). Explaining *why* something works, 
not just *what* to type and writing documentation.

**Where it was wrong, and how I caught it.** The assistant initially passed the
Event Hubs secret through `{{secrets/...}}` interpolation in the bundle it did not
resolve, and I verified the secret format in a notebook before switching to
`dbutils.secrets.get` inside the pipeline. It wrote an expectation against a column
name that no longer existed after a rename the pipelines analysis phase caught it.
It used `__file__` in a pipeline file, where it does not exist. Each time the fix came
from reading the actual error, not from asking again.

**Guardrails I kept.** Every created change ran on dev before prod. Secrets never
entered a prompt only their names. I asked for explanations of unfamiliar code and terms
before using it. Tests were written for the transformations the assistant helped draft 
so the assistants work is verified by code, not by trust. 
Git history records each step as a separate commit, so a bad suggestion is one revert away.

The honest summary: AI roughly doubled my speed on the parts I already
understood. On the parts I didn't, it let me learn them well enough to use them and
not well enough to claim I fully understand them yet. That gap is real and I would
rather name it than hide it. What AI did not change: I still had to read every
error message, check every result and make every decision myself. It can speed up
development or quietly derail it and the difference is whether you verify what it
gives you.

## 5. Known limitations

- The producer runs on a developer laptop, production would use an Azure Container App.
- Registry coverage is ~65%, unknown aircraft go to a sentinel row.
- Genie space instead of a custom RAG: a corpus of airport and aircraft-type
  descriptions from Wikipedia (links already in `silver_airports`) is deferred as
  a follow-up.
- One bounding box, no cross-border correlation.
- The bundle deploys everything that runs pipeline, job, governance policies. The
  dashboard and Genie space are serving-layer configuration created in the UI, AI/BI
  dashboards can be exported as JSON and added to the bundle, which is the planned
  next step.