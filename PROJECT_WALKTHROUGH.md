# Databricks project walkthrough — from basics to end to end

## First, what is in this repository?

This checkout contains eight Databricks interview-question/preparation PDFs, but no notebooks, application code, job definitions, or infrastructure. The detailed project story below is summarized from **`Databricks_Interview_Prep_OneShot.pdf`**. That PDF says its project facts came from three separate repositories (`coreservice-integration`, `hchb-integration`, and `bca-integration`), which are not included here. So treat project-specific names, schedules, and implementation details as what the prep document reports—not as facts independently verified against the running code.

## 1. The basic ideas

- **Databricks** is the platform used to develop and run data-processing jobs. It commonly runs Apache Spark workloads and manages notebooks, jobs, compute, tables, and permissions.
- **Apache Spark** is the distributed processing engine. Instead of one machine processing every row, Spark splits work across a cluster of machines.
- **Delta Lake** adds a transaction log and table-management features to data files in a data lake. In this project, Delta tables provide the durable tables that jobs can append to or update with `MERGE`.
- **Incremental ingestion** means processing only new or changed records instead of copying the whole source every run. A **watermark** is the saved position/time from which the next run continues.
- **Bronze / Silver / canonical data** describe increasing levels of usability: retain source-shaped data, standardize it, then maintain the shared business records used by downstream systems. The project guide has both a Landing and a Raw step under Bronze, followed by an Integration Silver step and shared canonical tables.
- **Stable IDs** matter because records refer to one another. An Episode, for example, must keep referring to the same Client even when that Client changes.

One important distinction: the source extraction described in the guide uses a stored procedure and a last-updated watermark. That is not automatically the same thing as reading a database's native CDC log. Later, **Delta Change Data Feed (CDF)** captures changes made to Delta tables. Those are two different change-capture mechanisms at two different parts of the pipeline.

## 2. What problem does the project solve?

The prep guide describes a healthcare data-integration platform. Different patient-management systems (PMSs) store information—such as clients, episodes of care, care teams, coverage, payors, authorizations, and visits—in different shapes. Downstream consumers should not each need a custom integration for every PMS.

The goal is to convert source-specific records into a shared, FHIR-aligned model, keep that model current in Databricks, and provide client/episode information to downstream services. One downstream described in the guide is **BCA**, which uses the data for billing.

In plain language: **take changed records from a care system, clean and standardize them, preserve their relationships, then deliver useful updates to billing and other consumers.**

## 3. The end-to-end picture

The guide describes this high-level flow for the HCHB integration:

```text
HCHB SQL Server
   │
   ▼
Per-entity stored procedures + saved watermark
   │  incremental pull (guide: every 15 min; Visit every 10 min)
   ▼
Bronze Landing — source-shaped rows, append and audit metadata
   │
   ▼
Bronze Raw — deduplicate and keep only genuinely changed records
   │
   ▼
Integration Silver — map to FHIR-style events and preserve stable IDs
   ├── missing parent? → reference retry queue → retry/re-pull parent
   ▼
Shared persistence — validate and MERGE into canonical Delta tables
   │
   ├── Delta CDF → Core services → business events → Kafka
   │
   └── BCA processing → client audit/staging → BayadaClient XML
                                             → SQL Server Service Broker
                                             → ODSDB billing queue
```

The exact BCA transport path should be checked against the original BCA repository. The prep guide describes Core services publishing events to Kafka and also describes BCA processing unprocessed Client/Episode records; it does not include the source code in this checkout to verify every connection between those components.

## 4. Each stage, explained simply

### Stage 1 — Read changes from the source

The HCHB database is owned by another team. According to the prep guide, the integration uses a stored procedure per entity rather than querying the source tables directly. A tracker called `entitysynctracker` stores the last successful time/position for each entity. Each run asks for records updated after that watermark and up to the current run's time.

The guide lists a 15-minute schedule for most entities and 10 minutes for Visit. It says the job writes the returned records to Landing before moving the watermark forward. That ordering matters: if the landing write fails, the next run can safely request the same time window instead of skipping records.

### Stage 2 — Keep a durable landing copy

The returned rows are appended to a Bronze Landing Delta table. The guide says the pipeline adds tracking/audit columns such as `_trace_id`, `_created_at`, `_job_id`, and `_is_processed`.

Think of Landing as the evidence of what arrived from the source. Keeping a source-shaped copy makes investigation and replay easier than retaining only the final transformed record.

### Stage 3 — Remove repeats and detect real changes

A source may return a record again even when its business data has not changed. The Raw step keeps the latest row per source ID and computes an MD5 checksum from the business columns. It compares that checksum with the saved checksum in Raw:

- New ID → treat it as a new record.
- Same ID, different checksum → treat it as a changed record.
- Same ID, same checksum → do not send an unchanged record through the rest of the pipeline.

Here MD5 is being used as a compact comparison value, not for password storage or security. The purpose is to reduce unnecessary downstream updates and duplicate billing work.

### Stage 4 — Standardize records in Integration Silver

The source data is mapped into the common, FHIR-style representation used by the integration. The guide mentions fields/structures such as identifiers, names, telecom/contact details, addresses, and extensions.

Before assigning an ID, the job looks up the source identifier in the canonical data:

- If the Client already exists, reuse its UUID and create an update event.
- If it does not exist, create a UUID and create a new-entity event.

The UUID must stay stable. Episodes, coverage, and care-team records can refer to the Client by that UUID; making a new UUID for every update would break those links.

### Stage 5 — Wait for missing parent records

Records can arrive out of order. An Episode may arrive before its Client or CareTeam; Coverage may arrive before its Client or Payor. The guide describes placing these records in a reference retry queue instead of persisting a broken relationship.

A retry job waits, re-pulls missing parent records by ID, and tries again. The guide lists a maximum of three re-pulls before an exhausted case is logged. This is a reliability feature: ordinary ordering delays can heal themselves without someone manually rerunning the whole pipeline.

### Stage 6 — Validate and upsert into shared canonical tables

A shared persistence layer cleans/maps the event, runs validations, and uses Delta `MERGE` to insert or update the canonical `core_entities` tables. This is the common source of truth used across PMS integrations.

The prep guide says trace IDs are used to make reprocessing safer and prevent the same event from causing unnecessary repeated updates. Invalid rows are logged; exactly how failed records are quarantined or retried needs to be checked in the source code.

### Stage 7 — Turn table changes into business events

The guide says canonical Delta tables have CDF enabled. Core services read changes by Delta table version, compare before/after images, classify the business change (for example, a contact-information update), and save their place in a control table such as `cdf_entity_offset`. A separate step publishes resulting events to Kafka.

This is different from the watermark used to pull from the source. The source watermark tracks source updates; the CDF offset tracks which Delta-table changes a downstream component has already handled.

### Stage 8 — Prepare and send the billing feed

For BCA, the prep guide describes using Client and Episode data plus related Coverage/CareTeam information to maintain audit/link records, rebuild a `BayadaClient` XML message when relevant data changes, and stage the message before publishing it to SQL Server Service Broker. It lists a batch size of 500 and says rows are marked processed only after the publish batch commits, so an interrupted batch can be retried.

The result is not just a table in Databricks: the standardized data is made usable by an operational downstream system that needs it for billing.

## 5. Follow one record all the way through

Suppose a Client's phone number changes in HCHB at 10:02:

1. The next scheduled source pull—10:15 in the guide's example—asks the stored procedure for records newer than the saved watermark. The Client is returned.
2. The row is appended to Bronze Landing with a new trace ID.
3. Raw computes its checksum. Since the phone number changed, the checksum differs from the saved version, so the change continues.
4. Silver finds the Client's existing source identifier, keeps the same UUID, and represents the row as an update.
5. The persistence layer validates it and merges the new contact details into the canonical Delta table.
6. Delta CDF exposes the table change. Core services can classify it as a contact-information change and emit a business event.
7. BCA detects that the linked Client data needs refreshing, rebuilds the XML, stages it, and publishes it to the billing queue. It marks the staging work complete after a successful commit.

That is the simplest way to remember the whole project: **extract → land → detect changes → standardize → validate/upsert → publish changes → build and send the billing message.**

## 6. Why the design uses these mechanisms

| Mechanism | Plain-English reason |
|---|---|
| Watermark | Avoid a full source reload and resume from the last successful pull. |
| Append-only Landing | Preserve what arrived for audit, debugging, and replay. |
| Checksum | Avoid processing an unchanged record repeatedly. |
| Stable UUID | Keep links between Client, Episode, Coverage, and other entities intact. |
| Reference retry queue | Handle records that arrive before their parent records. |
| Delta `MERGE` | Insert new canonical entities and update existing ones safely. |
| Delta CDF + offset | Let downstream services process table changes incrementally and track progress. |
| Trace IDs / processed flags | Trace a record across stages and make retries/restarts manageable. |
| Staging before publish | Separate message preparation from successful delivery to the operational queue. |

## 7. A short interview explanation

Use this as a structure, and change the first-person ownership statements to match what you personally did:

> “The project is a healthcare data-integration platform on Databricks. Different patient-management systems store Client, Episode, Coverage, and related data differently, so we bring their changes into a shared FHIR-aligned model. For the HCHB integration described in my project notes, stored procedures return incremental changes from SQL Server. We land them in Bronze, filter unchanged records with checksums, map them to stable-ID events in Silver, and validate/upsert them into canonical Delta tables. Core services process Delta changes and publish business events; the BCA flow uses Client and Episode data to build XML messages for the billing queue. My specific contribution was [state your real work here].”

## 8. Important facts to verify before an interview

The prep PDF itself warns not to guess about production volume, run times, exact personal ownership, or the names/count of PMS systems. This checkout does not contain the original source repositories to verify those details. Also, the prep PDF says AWS while flagging that a résumé says Azure Databricks; confirm the actual project/cloud and make your résumé and answers consistent.

It is also useful to understand the design's limits rather than present it as perfect. The prep document calls out that timestamp-based pulls may miss hard deletes, failed rows need a strong quarantine/alerting strategy, and control-flag filters can become expensive at scale. Explain only issues you have actually seen or can verify.

## 9. A sensible study order for the PDFs

1. Learn this walkthrough and the source-to-consumer flow.
2. Review SQL, Python, JSON, schemas, and basic ETL concepts.
3. Study Spark DataFrames, transformations/actions, joins, shuffle, and execution basics.
4. Study Delta tables, `MERGE`, schema evolution, CDF, watermarks, retries, and idempotency.
5. Study performance topics: small files, partitioning/clustering, predicate pushdown, AQE, and Spark UI.
6. Study governance/reporting topics: Unity Catalog, row/column security, Power BI, and cost.
7. Treat the Generative AI PDFs as a separate topic area after the data-engineering basics.
