USE job_warehouse;

-- 1. Job demand by industry
SELECT
    i.industry_name AS industry,
    COUNT(*) AS job_postings
FROM FactJobPosting f
JOIN DimIndustry i ON f.industry_id = i.industry_id
GROUP BY i.industry_name
ORDER BY job_postings DESC;

-- 2. Job demand by location
SELECT
    l.city AS location,
    COUNT(*) AS job_postings
FROM FactJobPosting f
JOIN DimLocation l ON f.location_id = l.location_id
GROUP BY l.city
ORDER BY job_postings DESC;

-- 3. Top hiring companies
SELECT
    c.company_name AS company,
    COUNT(*) AS job_postings
FROM FactJobPosting f
JOIN DimCompany c ON f.company_id = c.company_id
GROUP BY c.company_name
ORDER BY job_postings DESC
LIMIT 20;

-- 4. Salary trend by industry (monthly)
SELECT
    t.year,
    t.month,
    i.industry_name,
    AVG(f.salary_min) AS avg_salary_min,
    AVG(f.salary_max) AS avg_salary_max
FROM FactJobPosting f
JOIN DimTime t ON f.time_id = t.time_id
JOIN DimIndustry i ON f.industry_id = i.industry_id
WHERE f.salary_min IS NOT NULL
  AND f.salary_max IS NOT NULL
GROUP BY t.year, t.month, i.industry_name
ORDER BY t.year, t.month;

-- 5. Job posting trend by week
SELECT
    t.year,
    WEEK(t.posting_date, 1) AS week_of_year,
    COUNT(*) AS job_postings
FROM FactJobPosting f
JOIN DimTime t ON f.time_id = t.time_id
GROUP BY t.year, WEEK(t.posting_date, 1)
ORDER BY t.year, week_of_year;

-- 6. Agency comparison
SELECT
    s.agency_name AS agency,
    COUNT(*) AS job_postings
FROM FactJobPosting f
JOIN DimSourceAgency s ON f.source_id = s.source_id
GROUP BY s.agency_name
ORDER BY job_postings DESC;