-- star_schema.sql
-- Target platform: PostgreSQL (adjust syntax if using SQL Server/MySQL)

CREATE SCHEMA IF NOT EXISTS dw_job;
SET search_path TO dw_job;

-- =====================
-- Dimension tables
-- =====================

CREATE TABLE IF NOT EXISTS dim_time (
    time_key        DATE PRIMARY KEY,
    full_date       DATE NOT NULL,
    day             SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    quarter         SMALLINT NOT NULL,
    year            SMALLINT NOT NULL,
    week_of_year    SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_company (
    company_key     SERIAL PRIMARY KEY,
    company_name    VARCHAR(255) NOT NULL,
    company_name_std VARCHAR(255) NOT NULL,
    company_group   VARCHAR(255),
    industry_key    INTEGER,
    headquarters    VARCHAR(255),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_job (
    job_key         SERIAL PRIMARY KEY,
    job_title_raw   VARCHAR(255) NOT NULL,
    job_title_std   VARCHAR(255) NOT NULL,
    job_category    VARCHAR(100),
    job_function    VARCHAR(100),
    experience_band VARCHAR(100),
    education_band  VARCHAR(100),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_industry (
    industry_key    SERIAL PRIMARY KEY,
    industry_name_raw  VARCHAR(150) NOT NULL,
    industry_name_std  VARCHAR(150) NOT NULL,
    sector           VARCHAR(150),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (industry_name_std)
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_key    SERIAL PRIMARY KEY,
    country         VARCHAR(100) DEFAULT 'Cambodia',
    province        VARCHAR(100) NOT NULL,
    city            VARCHAR(100),
    region          VARCHAR(100),
    lat             DECIMAL(9,6),
    lon             DECIMAL(9,6),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (country, province, COALESCE(city, ''))
);

CREATE TABLE IF NOT EXISTS dim_source_agency (
    source_key      SERIAL PRIMARY KEY,
    source_name_raw VARCHAR(150) NOT NULL,
    source_name_std VARCHAR(150) NOT NULL,
    source_type     VARCHAR(100),
    url             VARCHAR(255),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_name_std)
);

-- =====================
-- Fact table
-- =====================

CREATE TABLE IF NOT EXISTS fact_job_posting (
    job_posting_key     BIGSERIAL PRIMARY KEY,
    time_key            DATE NOT NULL REFERENCES dim_time(time_key),
    company_key         INTEGER NOT NULL REFERENCES dim_company(company_key),
    job_key             INTEGER NOT NULL REFERENCES dim_job(job_key),
    industry_key        INTEGER NOT NULL REFERENCES dim_industry(industry_key),
    location_key        INTEGER NOT NULL REFERENCES dim_location(location_key),
    source_key          INTEGER NOT NULL REFERENCES dim_source_agency(source_key),
    job_count           SMALLINT DEFAULT 1,
    salary_min_usd      NUMERIC(12,2),
    salary_max_usd      NUMERIC(12,2),
    salary_currency     VARCHAR(10),
    salary_type         VARCHAR(50),  -- e.g., range, fixed, negotiable, undisclosed
    employment_type     VARCHAR(50),  -- Full-time, Part-time, etc.
    experience_level    VARCHAR(100),
    education_level     VARCHAR(100),
    posting_date        DATE NOT NULL,
    collected_at        TIMESTAMP,
    source_record_id    VARCHAR(255),
    hash_dedup          VARCHAR(64) NOT NULL,
    UNIQUE (hash_dedup)
);

CREATE INDEX IF NOT EXISTS ix_fact_job_posting_time ON fact_job_posting(time_key);
CREATE INDEX IF NOT EXISTS ix_fact_job_posting_company ON fact_job_posting(company_key);
CREATE INDEX IF NOT EXISTS ix_fact_job_posting_industry ON fact_job_posting(industry_key);
CREATE INDEX IF NOT EXISTS ix_fact_job_posting_location ON fact_job_posting(location_key);
CREATE INDEX IF NOT EXISTS ix_fact_job_posting_source ON fact_job_posting(source_key);

-- =====================
-- Helper views
-- =====================

CREATE OR REPLACE VIEW vw_salary_distribution AS
SELECT
    fi.industry_name_std,
    AVG(f.salary_min_usd) AS avg_salary_min_usd,
    AVG(f.salary_max_usd) AS avg_salary_max_usd,
    COUNT(*) AS postings
FROM fact_job_posting f
JOIN dim_industry fi USING (industry_key)
WHERE salary_min_usd IS NOT NULL AND salary_max_usd IS NOT NULL
GROUP BY fi.industry_name_std;

CREATE OR REPLACE VIEW vw_job_demand_by_province AS
SELECT
    dl.province,
    COUNT(*) AS job_count
FROM fact_job_posting f
JOIN dim_location dl USING (location_key)
GROUP BY dl.province;