-- staging_tables.sql
CREATE SCHEMA IF NOT EXISTS staging;
SET search_path TO staging;

CREATE TABLE IF NOT EXISTS job_postings_raw (
    source_file        VARCHAR(255),
    load_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp_collected TIMESTAMP,
    job_title          VARCHAR(255),
    company_name       VARCHAR(255),
    industry           VARCHAR(255),
    location_raw       VARCHAR(255),
    salary_raw         VARCHAR(255),
    job_type           VARCHAR(100),
    experience_level   VARCHAR(100),
    education_level    VARCHAR(100),
    posting_date_raw   VARCHAR(50),
    source_agency      VARCHAR(150),
    extra              JSONB
);

CREATE TABLE IF NOT EXISTS job_postings_cleaned (
    source_file             VARCHAR(255),
    load_timestamp          TIMESTAMP,
    job_title_raw           VARCHAR(255),
    job_title_std           VARCHAR(255),
    company_name_raw        VARCHAR(255),
    company_name_std        VARCHAR(255),
    industry_raw            VARCHAR(255),
    industry_std            VARCHAR(255),
    province                VARCHAR(100),
    city                    VARCHAR(100),
    salary_type             VARCHAR(50),
    salary_currency         VARCHAR(10),
    salary_min_usd          NUMERIC(12,2),
    salary_max_usd          NUMERIC(12,2),
    employment_type         VARCHAR(50),
    experience_level_std    VARCHAR(100),
    education_level_std     VARCHAR(100),
    posting_date            DATE,
    source_agency_raw       VARCHAR(150),
    source_agency_std       VARCHAR(150),
    hash_dedup              VARCHAR(64),
    PRIMARY KEY (hash_dedup)
);