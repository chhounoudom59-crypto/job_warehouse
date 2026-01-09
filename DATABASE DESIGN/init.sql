-- -- Initialize Job Warehouse Database
-- DROP DATABASE IF EXISTS job_warehouse;
-- CREATE DATABASE job_warehouse;
-- USE job_warehouse;

-- -- Staging Table
-- CREATE TABLE IF NOT EXISTS StagingJobPostings (
--     staging_id INT AUTO_INCREMENT PRIMARY KEY,
--     timestamp_col DATETIME,
--     job_title VARCHAR(255),
--     company_name VARCHAR(255),
--     industry VARCHAR(255),
--     location VARCHAR(100),
--     salary_range VARCHAR(100),
--     job_type VARCHAR(50),
--     experience_level VARCHAR(50),
--     education_level VARCHAR(100),
--     posting_date DATE,
--     source_agency VARCHAR(100),
--     salary_min DECIMAL(10,2),
--     salary_max DECIMAL(10,2),
--     is_processed BOOLEAN DEFAULT FALSE,
--     process_date DATETIME,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     INDEX idx_processed (is_processed),
--     INDEX idx_company_job (company_name(50), job_title(100)),
--     INDEX idx_post_date (posting_date)
-- );

-- -- Dimension Tables
-- CREATE TABLE IF NOT EXISTS DimTime (
--     time_id INT AUTO_INCREMENT PRIMARY KEY,
--     posting_date DATE NOT NULL UNIQUE,
--     posting_day INT,
--     posting_month INT,
--     posting_quarter INT,
--     posting_year INT,
--     posting_weekday VARCHAR(20),
--     is_weekend BOOLEAN,
--     INDEX idx_date (posting_date),
--     INDEX idx_year_month (posting_year, posting_month)
-- );

-- CREATE TABLE IF NOT EXISTS DimCompany (
--     company_id INT AUTO_INCREMENT PRIMARY KEY,
--     company_name VARCHAR(255) NOT NULL UNIQUE,
--     company_type VARCHAR(100) DEFAULT 'Private',
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     INDEX idx_company_name (company_name(100))
-- );

-- CREATE TABLE IF NOT EXISTS DimJob (
--     job_id INT AUTO_INCREMENT PRIMARY KEY,
--     job_title VARCHAR(255) NOT NULL,
--     job_type VARCHAR(50),
--     experience_level VARCHAR(50),
--     education_level VARCHAR(100),
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     INDEX idx_job_title (job_title(100)),
--     INDEX idx_experience (experience_level)
-- );

-- CREATE TABLE IF NOT EXISTS DimIndustry (
--     industry_id INT AUTO_INCREMENT PRIMARY KEY,
--     industry_name VARCHAR(255) NOT NULL UNIQUE,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     INDEX idx_industry_name (industry_name(100))
-- );

-- CREATE TABLE IF NOT EXISTS DimLocation (
--     location_id INT AUTO_INCREMENT PRIMARY KEY,
--     city VARCHAR(100) NOT NULL,
--     province VARCHAR(100),
--     UNIQUE KEY unique_location (city, province),
--     INDEX idx_city (city),
--     INDEX idx_province (province)
-- );

-- CREATE TABLE IF NOT EXISTS DimSourceAgency (
--     source_id INT AUTO_INCREMENT PRIMARY KEY,
--     agency_name VARCHAR(100) NOT NULL UNIQUE,
--     platform_type VARCHAR(50),
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     INDEX idx_agency_name (agency_name)
-- );

-- -- Fact Table
-- CREATE TABLE IF NOT EXISTS FactJobPosting (
--     fact_id INT AUTO_INCREMENT PRIMARY KEY,
--     time_id INT NOT NULL,
--     company_id INT NOT NULL,
--     job_id INT NOT NULL,
--     industry_id INT NOT NULL,
--     location_id INT NOT NULL,
--     source_id INT NOT NULL,
--     job_count INT DEFAULT 1,
--     salary_min DECIMAL(10,2),
--     salary_max DECIMAL(10,2),
--     salary_avg DECIMAL(10,2) AS ((salary_min + salary_max) / 2) STORED,
--     has_salary BOOLEAN AS (salary_min IS NOT NULL) STORED,
--     load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     FOREIGN KEY (time_id) REFERENCES DimTime(time_id),
--     FOREIGN KEY (company_id) REFERENCES DimCompany(company_id),
--     FOREIGN KEY (job_id) REFERENCES DimJob(job_id),
--     FOREIGN KEY (industry_id) REFERENCES DimIndustry(industry_id),
--     FOREIGN KEY (location_id) REFERENCES DimLocation(location_id),
--     FOREIGN KEY (source_id) REFERENCES DimSourceAgency(source_id),
--     INDEX idx_salary (salary_avg),
--     INDEX idx_load_date (load_date),
--     UNIQUE KEY unique_posting (time_id, company_id, job_id, industry_id, location_id, source_id)
-- );

-- -- Views for Analysis
-- CREATE OR REPLACE VIEW vw_job_summary AS
-- SELECT 
--     j.job_title,
--     c.company_name,
--     i.industry_name,
--     l.city,
--     t.posting_date,
--     f.salary_min,
--     f.salary_max,
--     f.salary_avg,
--     s.agency_name,
--     j.experience_level,
--     j.education_level
-- FROM FactJobPosting f
-- JOIN DimJob j ON f.job_id = j.job_id
-- JOIN DimCompany c ON f.company_id = c.company_id
-- JOIN DimIndustry i ON f.industry_id = i.industry_id
-- JOIN DimLocation l ON f.location_id = l.location_id
-- JOIN DimTime t ON f.time_id = t.time_id
-- JOIN DimSourceAgency s ON f.source_id = s.source_id;

-- CREATE OR REPLACE VIEW vw_industry_analysis AS
-- SELECT 
--     i.industry_name,
--     COUNT(*) as total_jobs,
--     SUM(CASE WHEN f.salary_min IS NOT NULL THEN 1 ELSE 0 END) as jobs_with_salary,
--     AVG(f.salary_avg) as avg_salary,
--     COUNT(DISTINCT c.company_id) as unique_companies
-- FROM FactJobPosting f
-- JOIN DimIndustry i ON f.industry_id = i.industry_id
-- JOIN DimCompany c ON f.company_id = c.company_id
-- GROUP BY i.industry_name;

-- -- Insert default source agencies
-- INSERT IGNORE INTO DimSourceAgency (agency_name, platform_type) VALUES
-- ('Facebook Page', 'Social Media'),
-- ('Jobify Cambodia', 'Job Portal'),
-- ('LinkedIn Cambodia', 'Professional Network'),
-- ('CamHR', 'Job Portal'),
-- ('BongThom', 'Classifieds'),
-- ('Khmer24 Jobs', 'Classifieds');

-- -- Insert default locations
-- INSERT IGNORE INTO DimLocation (city, province) VALUES
-- ('Phnom Penh', 'Phnom Penh'),
-- ('Siem Reap', 'Siem Reap'),
-- ('Kandal', 'Kandal'),
-- ('Preah Sihanouk', 'Preah Sihanouk'),
-- ('Kampong Speu', 'Kampong Speu'),
-- ('Battambang', 'Battambang');

-- -- Create ETL Log Table
-- CREATE TABLE IF NOT EXISTS ETL_Logs (
--     log_id INT AUTO_INCREMENT PRIMARY KEY,
--     log_type VARCHAR(50),
--     process_name VARCHAR(100),
--     record_count INT,
--     status VARCHAR(20),
--     error_message TEXT,
--     start_time DATETIME,
--     end_time DATETIME,
--     duration_seconds INT,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- DELIMITER $$

-- -- Stored Procedure for ETL
-- CREATE PROCEDURE sp_run_etl()
-- BEGIN
--     DECLARE v_start_time DATETIME;
--     DECLARE v_end_time DATETIME;
--     DECLARE v_records INT;
    
--     SET v_start_time = NOW();
    
--     -- Insert new time records
--     INSERT IGNORE INTO DimTime (posting_date, posting_day, posting_month, posting_quarter, posting_year, posting_weekday, is_weekend)
--     SELECT DISTINCT 
--         posting_date,
--         DAY(posting_date),
--         MONTH(posting_date),
--         QUARTER(posting_date),
--         YEAR(posting_date),
--         DAYNAME(posting_date),
--         DAYOFWEEK(posting_date) IN (1,7)
--     FROM StagingJobPostings
--     WHERE is_processed = FALSE;
    
--     -- Insert new companies
--     INSERT IGNORE INTO DimCompany (company_name)
--     SELECT DISTINCT company_name
--     FROM StagingJobPostings
--     WHERE is_processed = FALSE;
    
--     -- Insert new jobs
--     INSERT IGNORE INTO DimJob (job_title, job_type, experience_level, education_level)
--     SELECT DISTINCT 
--         job_title,
--         job_type,
--         experience_level,
--         education_level
--     FROM StagingJobPostings
--     WHERE is_processed = FALSE;
    
--     -- Insert new industries
--     INSERT IGNORE INTO DimIndustry (industry_name)
--     SELECT DISTINCT industry
--     FROM StagingJobPostings
--     WHERE is_processed = FALSE;
    
--     -- Insert new locations
--     INSERT IGNORE INTO DimLocation (city)
--     SELECT DISTINCT location
--     FROM StagingJobPostings
--     WHERE is_processed = FALSE;
    
--     -- Insert new source agencies
--     INSERT IGNORE INTO DimSourceAgency (agency_name)
--     SELECT DISTINCT source_agency
--     FROM StagingJobPostings
--     WHERE is_processed = FALSE;
    
--     -- Insert into fact table
--     INSERT INTO FactJobPosting (time_id, company_id, job_id, industry_id, location_id, source_id, salary_min, salary_max)
--     SELECT 
--         t.time_id,
--         c.company_id,
--         j.job_id,
--         i.industry_id,
--         l.location_id,
--         s.source_id,
--         st.salary_min,
--         st.salary_max
--     FROM StagingJobPostings st
--     JOIN DimTime t ON st.posting_date = t.posting_date
--     JOIN DimCompany c ON st.company_name = c.company_name
--     JOIN DimJob j ON st.job_title = j.job_title 
--         AND st.job_type = j.job_type
--         AND st.experience_level = j.experience_level
--     JOIN DimIndustry i ON st.industry = i.industry_name
--     JOIN DimLocation l ON st.location = l.city
--     JOIN DimSourceAgency s ON st.source_agency = s.agency_name
--     WHERE st.is_processed = FALSE
--     ON DUPLICATE KEY UPDATE 
--         salary_min = VALUES(salary_min),
--         salary_max = VALUES(salary_max);
    
--     -- Mark as processed
--     UPDATE StagingJobPostings 
--     SET is_processed = TRUE, 
--         process_date = NOW()
--     WHERE is_processed = FALSE;
    
--     SET v_end_time = NOW();
--     SET v_records = ROW_COUNT();
    
--     -- Log the ETL run
--     INSERT INTO ETL_Logs (log_type, process_name, record_count, status, start_time, end_time, duration_seconds)
--     VALUES ('INFO', 'ETL Pipeline', v_records, 'COMPLETED', v_start_time, v_end_time, TIMESTAMPDIFF(SECOND, v_start_time, v_end_time));
    
-- END$$

-- DELIMITER ;