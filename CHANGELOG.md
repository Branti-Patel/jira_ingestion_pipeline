# Changelog

All notable changes to this project will be documented here.

---

## v2_bronze_ingestion

### Added

* Multi-project Jira issue ingestion
* Sprint and board data extraction using Jira Agile API
* Raw JSON storage for issues and sprints (Bronze layer)
* Ingestion timestamp for tracking data load

### Data Assets Overview

**Tables**

* `workspace.default.workitems_raw` → Stores raw Jira issues data (full JSON + metadata)
* `workspace.default.sprints_raw` → Stores raw sprint data (full JSON + metadata)

**DataFrames**

* `df` → Issues dataframe (raw issue data before writing to Delta)
* `s_df` → Sprints dataframe (raw sprint data before writing to Delta)

**Intermediate Collections**

* `rows` → List storing issue records (raw JSON + metadata)
* `sprint_rows` → List storing sprint records (raw JSON + metadata)

### Changed

* Shifted from structured transformation to raw ingestion approach (Bronze layer)
* Introduced support for multiple Jira project keys

### Issues / Limitations

* Issues pagination not fully implemented
* Minor inconsistencies in function usage

---

## v1_initial

### Added

* Basic Jira API integration
* Used only 1 proj key in API
* Data extraction and transformation using PySpark
* Delta table upsert (merge) logic

### Notes

* Initial working version of the pipeline
