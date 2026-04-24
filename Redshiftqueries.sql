
-- Create Schemas
CREATE SCHEMA IF NOT EXISTS healthcare_gold;
CREATE SCHEMA IF NOT EXISTS project_output;


-- Dimension Tables
-- ── dim_hospital ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS healthcare_gold.dim_hospital (
    hospital_id     VARCHAR(10)     NOT NULL,
    hospital_name   VARCHAR(200),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100),
    PRIMARY KEY (hospital_id)
)
DISTSTYLE KEY
DISTKEY (hospital_id)
SORTKEY (hospital_id);

-- ── dim_disease ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS healthcare_gold.dim_disease (
    disease_id      INTEGER         NOT NULL,
    disease_name    VARCHAR(200),
    subgrp_id       VARCHAR(10),
    PRIMARY KEY (disease_id)
)
DISTSTYLE KEY
DISTKEY (disease_name)
SORTKEY (disease_name);

-- ── dim_subgroup ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS healthcare_gold.dim_subgroup (
    subgrp_id           VARCHAR(10)     NOT NULL,
    subgrp_name         VARCHAR(200),
    monthly_premium     DECIMAL(10,2),
    grp_id              VARCHAR(10),
    PRIMARY KEY (subgrp_id)
)
DISTSTYLE KEY
DISTKEY (subgrp_id)
SORTKEY (subgrp_id);

-- ── dim_group ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS healthcare_gold.dim_group (
    grp_id              VARCHAR(10)     NOT NULL,
    grp_name            VARCHAR(200),
    grp_type            VARCHAR(20),
    country             VARCHAR(100),
    city                VARCHAR(100),
    premium_written     DECIMAL(14,2),
    year                INTEGER,
    PRIMARY KEY (grp_id)
)
DISTSTYLE KEY
DISTKEY (grp_id)
SORTKEY (grp_type);

-- ── dim_subscriber ────────────────────────────────────
CREATE TABLE IF NOT EXISTS healthcare_gold.dim_subscriber (
    sub_id          VARCHAR(20)     NOT NULL,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    birth_date      DATE,
    gender          VARCHAR(10),
    phone           VARCHAR(20),
    street          VARCHAR(200),
    city            VARCHAR(100),
    country         VARCHAR(100),
    zip_code        VARCHAR(20),
    subgrp_id       VARCHAR(10),
    elig_ind        VARCHAR(5),
    eff_date        DATE,
    term_date       DATE,
    PRIMARY KEY (sub_id)
)
DISTSTYLE KEY
DISTKEY (sub_id)
SORTKEY (sub_id);

-- ── dim_patient ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS healthcare_gold.dim_patient (
    patient_id          VARCHAR(20)     NOT NULL,
    patient_name        VARCHAR(200),
    patient_gender      VARCHAR(10),
    patient_birth_date  DATE,
    patient_phone       VARCHAR(20),
    disease_name        VARCHAR(200),
    city                VARCHAR(100),
    hospital_id         VARCHAR(10),
    hospital_name       VARCHAR(200),
    PRIMARY KEY (patient_id)
)
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (patient_id);

-- ── dim_date ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS healthcare_gold.dim_date (
    date_id         INTEGER         NOT NULL,
    full_date       DATE,
    day             INTEGER,
    month           INTEGER,
    month_name      VARCHAR(20),
    quarter         INTEGER,
    year            INTEGER,
    PRIMARY KEY (date_id)
)
DISTSTYLE ALL
SORTKEY (full_date);

-- ── fact_claims ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS healthcare_gold.fact_claims (
    claim_id            INTEGER         NOT NULL,
    patient_id          VARCHAR(20),
    sub_id              VARCHAR(20),
    disease_name        VARCHAR(200),
    hospital_id         VARCHAR(10),
    subgrp_id           VARCHAR(10),
    grp_id              VARCHAR(10),
    date_id             INTEGER,
    claim_type          VARCHAR(50),
    claim_amount        DECIMAL(12,2),
    claim_or_rejected   VARCHAR(5),
    claim_date          DATE,
    PRIMARY KEY (claim_id)
)
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (claim_date);

-- ══════════════════════════════════════════════════════
-- project_output tables (one per use case)
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS project_output.max_claims_by_disease (
    disease_name    VARCHAR(200),
    total_claims    INTEGER
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS project_output.subscribers_under_30 (
    sub_id          VARCHAR(20),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    age             INTEGER,
    subgrp_id       VARCHAR(10)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS project_output.group_max_subgroups (
    grp_id          VARCHAR(10),
    grp_name        VARCHAR(200),
    subgroup_count  INTEGER
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS project_output.hospital_most_patients (
    hospital_name   VARCHAR(200),
    patient_count   INTEGER
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS project_output.most_subscribed_subgroup (
    subgrp_id           VARCHAR(10),
    subgrp_name         VARCHAR(200),
    subscription_count  INTEGER
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS project_output.total_rejected_claims (
    total_rejected  INTEGER
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS project_output.top_claim_city (
    city            VARCHAR(100),
    claim_count     INTEGER
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS project_output.govt_vs_private (
    grp_type            VARCHAR(20),
    subscriber_count    INTEGER
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS project_output.avg_monthly_premium (
    average_monthly_premium DECIMAL(10,2)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS project_output.most_profitable_group (
    grp_id              VARCHAR(10),
    grp_name            VARCHAR(200),
    total_revenue       DECIMAL(14,2)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS project_output.patients_under_18_cancer (
    patient_id      VARCHAR(20),
    patient_name    VARCHAR(200),
    age             INTEGER,
    disease_name    VARCHAR(200)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS project_output.cashless_high_charges (
    patient_id      VARCHAR(20),
    sub_id          VARCHAR(20),
    elig_ind        VARCHAR(5),
    claim_amount    DECIMAL(12,2)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS project_output.female_over_40_knee_surgery (
    patient_id          VARCHAR(20),
    patient_name        VARCHAR(200),
    patient_gender      VARCHAR(10),
    patient_birth_date  DATE,
    disease_name        VARCHAR(200),
    hospital_name       VARCHAR(200)
) DISTSTYLE ALL;


-- ── dim_hospital ──────────────────────────────────────
COPY healthcare_gold.dim_hospital
FROM 's3://healthcare-insurance-project/gold/dim_hospital/'
IAM_ROLE 'arn:aws:iam::505038841786:role/redshift_admin'
FORMAT AS PARQUET;

-- ── dim_disease ───────────────────────────────────────
COPY healthcare_gold.dim_disease
FROM 's3://healthcare-insurance-project/gold/dim_disease/'
IAM_ROLE 'arn:aws:iam::505038841786:role/redshift_admin'
FORMAT AS PARQUET;

-- ── dim_subgroup ──────────────────────────────────────
COPY healthcare_gold.dim_subgroup
FROM 's3://healthcare-insurance-project/gold/dim_subgroup/'
IAM_ROLE 'arn:aws:iam::505038841786:role/redshift_admin'
FORMAT AS PARQUET;

-- ── dim_group ─────────────────────────────────────────
COPY healthcare_gold.dim_group
FROM 's3://healthcare-insurance-project/gold/dim_group/'
IAM_ROLE 'arn:aws:iam::505038841786:role/redshift_admin'
FORMAT AS PARQUET;

-- ── dim_subscriber ────────────────────────────────────
COPY healthcare_gold.dim_subscriber
FROM 's3://healthcare-insurance-project/gold/dim_subscriber/'
IAM_ROLE 'arn:aws:iam::505038841786:role/redshift_admin'
FORMAT AS PARQUET;

-- ── dim_patient ───────────────────────────────────────
COPY healthcare_gold.dim_patient
FROM 's3://healthcare-insurance-project/gold/dim_patient/'
IAM_ROLE 'arn:aws:iam::505038841786:role/redshift_admin'
FORMAT AS PARQUET;

-- ── dim_date ──────────────────────────────────────────
COPY healthcare_gold.dim_date
FROM 's3://healthcare-insurance-project/gold/dim_date/'
IAM_ROLE 'arn:aws:iam::505038841786:role/redshift_admin'
FORMAT AS PARQUET;

-- ── fact_claims ───────────────────────────────────────
COPY healthcare_gold.fact_claims
FROM 's3://healthcare-insurance-project/gold/fact_claims/'
IAM_ROLE 'arn:aws:iam::505038841786:role/redshift_admin'
FORMAT AS PARQUET;

-- Check row counts for all tables
SELECT 'dim_hospital'   AS table_name, COUNT(*) AS row_count FROM healthcare_gold.dim_hospital
UNION ALL
SELECT 'dim_disease',   COUNT(*) FROM healthcare_gold.dim_disease
UNION ALL
SELECT 'dim_subgroup',  COUNT(*) FROM healthcare_gold.dim_subgroup
UNION ALL
SELECT 'dim_group',     COUNT(*) FROM healthcare_gold.dim_group
UNION ALL
SELECT 'dim_subscriber',COUNT(*) FROM healthcare_gold.dim_subscriber
UNION ALL
SELECT 'dim_patient',   COUNT(*) FROM healthcare_gold.dim_patient
UNION ALL
SELECT 'dim_date',      COUNT(*) FROM healthcare_gold.dim_date
UNION ALL
SELECT 'fact_claims',   COUNT(*) FROM healthcare_gold.fact_claims
ORDER BY table_name;


-- ── UC-01: Disease with most claims ───────────────────
INSERT INTO project_output.max_claims_by_disease
SELECT disease_name, COUNT(*) AS total_claims
FROM healthcare_gold.fact_claims
GROUP BY disease_name
ORDER BY total_claims DESC
LIMIT 7;

SELECT DISTINCT * FROM project_output.max_claims_by_disease;


-- ── UC-02: Subscribers under 30 with any subgroup ─────
INSERT INTO project_output.subscribers_under_30
SELECT 
    s.sub_id,
    s.first_name,
    s.last_name,
    DATEDIFF(year, CAST(s.birth_date AS DATE), CURRENT_DATE) AS age,
    s.subgrp_id
FROM healthcare_gold.dim_subscriber s
WHERE DATEDIFF(year, CAST(s.birth_date AS DATE), CURRENT_DATE) < 30
AND s.subgrp_id IS NOT NULL
AND s.subgrp_id != 'NA';

SELECT * FROM project_output.subscribers_under_30;

-- ── UC-03: Group with most subgroups ──────────────────
INSERT INTO project_output.group_max_subgroups
SELECT 
    g.grp_id,
    g.grp_name,
    COUNT(s.subgrp_id) AS subgroup_count
FROM healthcare_gold.dim_group g
JOIN healthcare_gold.dim_subgroup s ON g.grp_id = s.grp_id
GROUP BY g.grp_id, g.grp_name
ORDER BY subgroup_count DESC
LIMIT 1;

SELECT * FROM project_output.group_max_subgroups;

-- ── UC-04: Hospital with most patients ────────────────
INSERT INTO project_output.hospital_most_patients
SELECT 
    hospital_name,
    COUNT(*) AS patient_count
FROM healthcare_gold.dim_patient
GROUP BY hospital_name
ORDER BY patient_count DESC
LIMIT 1;

SELECT DISTINCT * FROM project_output.hospital_most_patients;

-- ── UC-05: Most subscribed subgroup ───────────────────
INSERT INTO project_output.most_subscribed_subgroup
SELECT 
    s.subgrp_id,
    sg.subgrp_name,
    COUNT(*) AS subscription_count
FROM healthcare_gold.dim_subscriber s
JOIN healthcare_gold.dim_subgroup sg ON s.subgrp_id = sg.subgrp_id
GROUP BY s.subgrp_id, sg.subgrp_name
ORDER BY subscription_count DESC
LIMIT 1;

SELECT * FROM project_output.most_subscribed_subgroup;

-- ── UC-06: Total rejected claims ──────────────────────
INSERT INTO project_output.total_rejected_claims
SELECT COUNT(*) AS total_rejected
FROM healthcare_gold.fact_claims
WHERE claim_or_rejected = 'Y';

SELECT * FROM project_output.total_rejected_claims;

-- ── UC-07: City with most claims ──────────────────────
INSERT INTO project_output.top_claim_city
SELECT 
    p.city,
    COUNT(*) AS claim_count
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_patient p ON f.patient_id = p.patient_id
GROUP BY p.city
ORDER BY claim_count DESC
LIMIT 1;

SELECT * FROM project_output.top_claim_city;

-- ── UC-08: Govt vs Private subscriptions ──────────────
INSERT INTO project_output.govt_vs_private
SELECT 
    g.grp_type,
    COUNT(DISTINCT s.sub_id) AS subscriber_count
FROM healthcare_gold.dim_subscriber s
JOIN healthcare_gold.dim_subgroup sg ON s.subgrp_id = sg.subgrp_id
JOIN healthcare_gold.dim_group g ON sg.grp_id = g.grp_id
GROUP BY g.grp_type
ORDER BY subscriber_count DESC;

SELECT * FROM project_output.govt_vs_private;

-- ── UC-09: Average monthly premium ────────────────────
INSERT INTO project_output.avg_monthly_premium
SELECT 
    ROUND(AVG(CAST(sg.monthly_premium AS DECIMAL(10,2))), 2) AS average_monthly_premium
FROM healthcare_gold.dim_subscriber s
JOIN healthcare_gold.dim_subgroup sg ON s.subgrp_id = sg.subgrp_id;

SELECT * FROM project_output.avg_monthly_premium;

-- ── UC-10: Most profitable group ──────────────────────
INSERT INTO project_output.most_profitable_group
SELECT 
    g.grp_id,
    g.grp_name,
    SUM(f.claim_amount) AS total_revenue
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_subgroup sg ON f.subgrp_id = sg.subgrp_id
JOIN healthcare_gold.dim_group g ON sg.grp_id = g.grp_id
GROUP BY g.grp_id, g.grp_name
ORDER BY total_revenue DESC
LIMIT 1;

SELECT * FROM project_output.most_profitable_group;

-- ── UC-11: Patients under 18 with cancer ──────────────
INSERT INTO project_output.patients_under_18_cancer
SELECT 
    patient_id,
    patient_name,
    DATEDIFF(year, CAST(patient_birth_date AS DATE), CURRENT_DATE) AS age,
    disease_name
FROM healthcare_gold.dim_patient
WHERE DATEDIFF(year, CAST(patient_birth_date AS DATE), CURRENT_DATE) < 18
AND LOWER(disease_name) LIKE '%cancer%';

SELECT * FROM project_output.patients_under_18_cancer;

-- ── UC-12: Cashless patients with charges >= 50000 ────
INSERT INTO project_output.cashless_high_charges
SELECT 
    f.patient_id,
    f.sub_id,
    s.elig_ind,
    f.claim_amount
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_subscriber s ON f.sub_id = s.sub_id
WHERE s.elig_ind = 'Y'
AND f.claim_amount >= 50000;

SELECT * FROM project_output.cashless_high_charges;

-- ── UC-13: Female patients over 40 with knee surgery ──
INSERT INTO project_output.female_over_40_knee_surgery
SELECT 
    patient_id,
    patient_name,
    patient_gender,
    patient_birth_date,
    disease_name,
    hospital_name
FROM healthcare_gold.dim_patient
WHERE patient_gender = 'Female'
AND DATEDIFF(year, CAST(patient_birth_date AS DATE), CURRENT_DATE) > 40
AND LOWER(disease_name) LIKE '%knee%';

SELECT * FROM project_output.female_over_40_knee_surgery;