# Healthcare Insurance Big Data Analytics Pipeline
> End-to-end Big Data pipeline built on AWS following **Medallion Architecture** (Bronze → Silver → Gold) to analyze healthcare insurance data and answer 13 business use cases.

## Project Overview
A healthcare insurance company needed to analyze customer and claims data to identify trends, track hospital utilization, understand subscriber behavior, and find the most profitable policy groups. This pipeline ingests 8 raw datasets, cleans them with PySpark, models them into a Star Schema, and stores 13 analytical results in AWS Redshift.

## Architecture

```
Raw CSV/JSON        PySpark Clean      Star Schema       SQL Analytics
┌──────────┐       ┌──────────┐       ┌──────────┐      ┌──────────┐
│  BRONZE  │──────►│  SILVER  │──────►│   GOLD   │─────►│REDSHIFT  │
│  AWS S3  │       │  AWS S3  │       │  AWS S3  │      │ 13 Use   │
│ 8 Raw    │       │ 8 Clean  │       │ 7 Dim +  │      │ Cases    │
│ Datasets │       │ Parquet  │       │ 1 Fact   │      │ Answered │
└──────────┘       └──────────┘       └──────────┘      └──────────┘
              AWS Glue (PySpark)            COPY Command
```

## Tech Stack

| Technology | Purpose |
|---|---|
| AWS S3 | Bronze / Silver / Gold layer storage |
| AWS Glue | PySpark ETL notebooks |
| AWS Redshift Serverless | Star schema and analytics queries |
| PySpark | Data cleaning and transformation |
| SQL | 13 business use case queries |
| Apache Parquet | Columnar format for Silver and Gold layers |
| GitHub | Version control |
| Jira | Sprint planning (2-week Scrum) |

## Datasets
8 source files — Patient records, Subscribers, Claims (JSON), Hospitals, Diseases, Groups, Subgroups, Group-Subgroup mapping.

**Key data quality fixes:**
- Replaced nulls → `NA` across all datasets
- Fixed string `"NaN"` values in claims JSON
- Renamed `sub _id` column (had a space) → `sub_id`
- Cast all numeric columns to proper decimal/integer types
  
## Star Schema — Gold Layer

```
             dim_date
                │
  dim_disease   │    dim_hospital
          \     │     /
           ▼    ▼    ▼
dim_subscriber──► fact_claims ◄── dim_patient
                │
           dim_subgroup
                │
           dim_group
```

**7 Dimension Tables + 1 Fact Table** loaded into Redshift with DISTKEY and SORTKEY for query optimization.

## Business Use Cases (13 Total)

| # | Question | Result |
|---|---|---|
| UC-01 | Disease with most claims | Pet Allergy — 3 claims |
| UC-02 | Subscribers under age 30 | 1 subscriber found |
| UC-03 | Group with most subgroups | Liberty General Insurance |
| UC-04 | Hospital with most patients | Manipal Hospitals — 9 patients |
| UC-05 | Most subscribed subgroup | Therapy (S104) — 13 subscriptions |
| UC-06 | Total rejected claims | Stored in project_output |
| UC-07 | City with most claims | Bihar Sharif — 2 claims |
| UC-08 | Govt vs Private subscriptions | Private: 80 / Govt: 18 |
| UC-09 | Average monthly premium | Rs. 1,867.34 |
| UC-10 | Most profitable group | Raheja QBE — Rs. 808,265 |
| UC-11 | Patients under 18 with cancer | No records in sample |
| UC-12 | Cashless patients ≥ Rs. 50,000 | 17 patients |
| UC-13 | Female >40 with knee surgery | No records in sample |

All results stored in `project_output` schema in Redshift — one table per use case.

## Key Highlights

- Medallion Architecture — Bronze, Silver, Gold layers in AWS S3
- PySpark on AWS Glue — cleaned 8 datasets with null handling, deduplication, and type casting
- Star Schema — 7 dim tables + 1 fact table with proper DISTKEY/SORTKEY
- Redshift COPY command — loaded Gold Parquet directly from S3
- 13 business use cases — results stored in `project_output` schema
- 2-week Scrum sprint managed in Jira
