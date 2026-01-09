
# Job Recruitment Data Warehouse Project (Cambodia)

## 📊 Project Overview

This project implements a **complete Data Warehouse (DW) solution** to analyze job recruitment trends in **Cambodia**.
It uses a **star schema design**, an **ETL pipeline**, **OLAP analysis**, and is **ready for business intelligence tools** such as Power BI.

The system consolidates job postings from multiple sources into a centralized analytical database to support data-driven decision-making.

---

## 🎯 Project Objectives

* Analyze hiring trends across industries and locations
* Support HR, government, and educational decision-making
* Apply real-world **Data Warehousing and OLAP concepts**
* Demonstrate ETL pipeline implementation using Python and MySQL

---

## 🚀 Project Features

* **Data Collection**: Job postings consolidated from multiple sources
* **ETL Pipeline**: Automated Extract–Transform–Load process
* **Data Warehouse**: MySQL-based star schema
* **OLAP Analysis**: Multi-dimensional analysis using SQL
* **Business Intelligence Ready**: Compatible with Power BI
* **Dockerized**: Easy setup using Docker and Docker Compose

---

## 📁 Project Structure

```
DATA-WAREHOUSE-PROJECT/
├── docker/
│   ├── docker-compose.yml
│   └── mysql/
│       └── my.cnf
├── DATA COLLECTION LAYER/
│   └── Source Systems/
│       └── job.csv
├── DATABASE DESIGN/
│   ├── star_schema.sql
│   └── staging_tables.sql
├── ETL/
│   ├── etl_config.yaml
│   ├── etl_job_postings.py
│   └── helpers/
│       └── cleaning_rules.py
├── ANALYSIS/
│   ├── olap_queries.sql
│   └── powerbi_measure_notes.md
├── DOCUMENTATION/
│   ├── data_cleaning_rules.md
│   ├── project_methodology.md
│   └── presentation_outline.md
├── .env
├── requirements.txt
└── README.md
```

---

## 🛠️ Technologies Used

* **Database**: MySQL
* **ETL**: Python (Pandas, MySQL Connector)
* **Containerization**: Docker & Docker Compose
* **Analysis**: SQL (OLAP Queries)
* **BI Tool**: Power BI (optional)

---

## 🔧 Setup Instructions

### 1️⃣ Prerequisites

* Docker & Docker Compose
* Python 3.8 or higher

### 2️⃣ Create Project Structure

```bash
mkdir -p DATA-WAREHOUSE-PROJECT
cd DATA-WAREHOUSE-PROJECT
mkdir -p "DATA COLLECTION LAYER/Source Systems" DATABASE\ DESIGN ETL/helpers ANALYSIS DOCUMENTATION docker/mysql
```

### 3️⃣ Add Dataset

Place `job.csv` in:

```
DATA COLLECTION LAYER/Source Systems/job.csv
```

### 4️⃣ Start Database Services

```bash
cd docker
docker-compose up -d
```

### 5️⃣ Install Python Dependencies

```bash
cd ..
python -m venv venv
venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

### 6️⃣ Run ETL Pipeline

```bash
python "ETL/etl_job_postings.py"
```

### 7️⃣ Run OLAP Analysis

```bash
mysql -h localhost -P 3307 -u admin -padmin123 job_warehouse < "ANALYSIS/olap_queries.sql"
```

---

## 🔗 Access Information

* **MySQL**: `localhost:3307`
* **Username**: `admin`
* **Password**: `admin123`
* **phpMyAdmin**: `http://localhost:8081`

---

## 📊 Data Warehouse Schema

### Fact Table

* **FactJobPosting**

### Dimension Tables

* **DimTime**
* **DimCompany**
* **DimJob**
* **DimIndustry**
* **DimLocation**
* **DimSourceAgency**

### Views

* `vw_job_summary`
* `vw_industry_analysis`

---

## 🔄 ETL Process

1. **Extract**: Read job data from CSV
2. **Transform**:

   * Salary normalization
   * Location standardization
   * Industry classification
   * Experience level mapping
3. **Load**:

   * Staging tables
   * Dimension tables
   * Fact table
4. **Validate**:

   * Data quality checks
   * ETL logs

---

## 📈 Sample OLAP Queries

```sql
-- Top industries by job count
SELECT i.industry_name, COUNT(*) AS total_jobs
FROM FactJobPosting f
JOIN DimIndustry i ON f.industry_id = i.industry_id
GROUP BY i.industry_name
ORDER BY total_jobs DESC;
```

---

## 📊 Business Insights

* Most jobs are concentrated in **Phnom Penh**
* **Banking, IT, and Retail** dominate job postings
* Salary ranges from **$150 to $4,000+**
* Most roles require **2–5 years experience**
* Bachelor's degree is the most common requirement

---

## 🎯 Business Applications

* **HR Teams**: Hiring trend analysis
* **Job Seekers**: Market salary awareness
* **Universities**: Curriculum alignment
* **Government**: Labor market monitoring

---

## 📚 Educational Value

This project demonstrates:

* Star schema design
* ETL pipeline development
* OLAP analysis
* SQL optimization
* Docker-based deployment
* Real-world data analytics workflow

---

## 🔮 Future Enhancements

* Live API data integration
* Machine learning for salary prediction
* Interactive web dashboard
* Automated report generation
* Multi-country labor market analysis

---

## 📄 License

Educational use only.

---

**Project Status**: ✅ Production Ready
**Last Updated**: January 2024
**Data Scope**: 126 Job Records
**Country**: Cambodia 🇰🇭

