import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
import yaml
from datetime import datetime
import re
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class JobETL:
    def __init__(self):
        self.config = self.load_config()
        self.connection = None
        self.setup_database_connection()
        
    def load_config(self):
        """Load configuration from YAML file"""
        config_path = os.path.join(os.path.dirname(__file__), 'etl_config.yaml')
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def setup_database_connection(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', 3307)),
                database=os.getenv('DB_NAME', 'job_warehouse'),
                user=os.getenv('DB_USER', 'admin'),
                password=os.getenv('DB_PASSWORD', 'admin123')
            )
            logger.info("Connected to MySQL database")
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise
    
    def extract_salary(self, salary_text):
        """Extract salary range from text"""
        if pd.isna(salary_text) or not salary_text:
            return None, None
        
        salary_str = str(salary_text)
        
        # Skip non-disclosed salaries
        if 'Not disclosed' in salary_str or 'Negotiable' in salary_str:
            return None, None
        
        # Pattern for USD ranges
        patterns = [
            r'USD\s*(\d{1,3}(?:,\d{3})*)\s*[–\-]\s*(\d{1,3}(?:,\d{3})*)',  # USD 800 – 1,200
            r'USD\s*(\d+)\s*-\s*(\d+)',  # USD 300 - 500
            r'(\d+)\s*-\s*(\d+)\s*USD',  # 300-500 USD
        ]
        
        for pattern in patterns:
            match = re.search(pattern, salary_str)
            if match:
                min_sal = float(match.group(1).replace(',', ''))
                max_sal = float(match.group(2).replace(',', ''))
                return min_sal, max_sal
        
        # Handle single values like "USD 130 - 150"
        single_pattern = r'USD\s*(\d+)\s*-\s*(\d+)'
        match = re.search(single_pattern, salary_str)
        if match:
            return float(match.group(1)), float(match.group(2))
        
        return None, None
    
    def clean_data(self, df):
        """Clean and transform the data"""
        # Make a copy to avoid SettingWithCopyWarning
        df = df.copy()
        
        # Convert date columns
        df['Posting Date'] = pd.to_datetime(df['Posting Date'], errors='coerce').dt.date
        df['Posting Date'] = df['Posting Date'].fillna(pd.Timestamp.now().date())
        
        # Extract salary
        df[['salary_min', 'salary_max']] = df['Salary'].apply(
            lambda x: pd.Series(self.extract_salary(x))
        )
        
        # Replace NaN with None for database compatibility
        df['salary_min'] = df['salary_min'].where(pd.notna(df['salary_min']), None)
        df['salary_max'] = df['salary_max'].where(pd.notna(df['salary_max']), None)
        
        # Clean text fields and replace NaN with empty strings
        df['Job Title'] = df['Job Title'].fillna('').str.strip().str.title()
        df['Company Name'] = df['Company Name'].fillna('').str.strip()
        df['Industry'] = df['Industry'].fillna('').str.strip()
        df['Location'] = df['Location'].fillna('').str.strip()
        df['Job Type'] = df['Job Type'].fillna('').str.strip()
        df['Experience Level'] = df['Experience Level'].fillna('').str.strip()
        df['Education Level'] = df['Education Level'].fillna('').str.strip()
        df['Source Agency'] = df['Source Agency'].fillna('').str.strip()
        df['Salary'] = df['Salary'].fillna('').str.strip()
        
        # Apply mappings
        industry_mapping = self.config.get('cleaning_rules', {}).get('industry_mapping', {})
        df['Industry'] = df['Industry'].replace(industry_mapping)
        
        location_mapping = self.config.get('cleaning_rules', {}).get('location_mapping', {})
        df['Location'] = df['Location'].replace(location_mapping)
        
        return df
    
    def load_to_staging(self, df):
        """Load data to staging table"""
        try:
            cursor = self.connection.cursor()
            
            # Clear old unprocessed data
            cursor.execute("DELETE FROM StagingJobPostings WHERE is_processed = FALSE")
            
            # Insert data
            insert_query = """
                INSERT INTO StagingJobPostings (
                    timestamp_col, job_title, company_name, industry, location,
                    salary_range, job_type, experience_level, education_level,
                    posting_date, source_agency, salary_min, salary_max
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            records = []
            for _, row in df.iterrows():
                record = (
                    datetime.now(),
                    row['Job Title'] if pd.notna(row['Job Title']) else '',
                    row['Company Name'] if pd.notna(row['Company Name']) else '',
                    row['Industry'] if pd.notna(row['Industry']) else '',
                    row['Location'] if pd.notna(row['Location']) else '',
                    row['Salary'] if pd.notna(row['Salary']) else '',
                    row['Job Type'] if pd.notna(row['Job Type']) else '',
                    row['Experience Level'] if pd.notna(row['Experience Level']) else '',
                    row['Education Level'] if pd.notna(row['Education Level']) else '',
                    row['Posting Date'] if pd.notna(row['Posting Date']) else None,
                    row['Source Agency'] if pd.notna(row['Source Agency']) else '',
                    row['salary_min'] if pd.notna(row['salary_min']) else None,
                    row['salary_max'] if pd.notna(row['salary_max']) else None
                )
                records.append(record)
            
            cursor.executemany(insert_query, records)
            self.connection.commit()
            
            logger.info(f"Loaded {len(records)} records to staging")
            return len(records)
            
        except Error as e:
            logger.error(f"Error loading to staging: {e}")
            self.connection.rollback()
            raise
    
    def run_etl_pipeline(self):
        """Execute the complete ETL pipeline"""
        logger.info("Starting ETL pipeline...")
        
        try:
            # Step 1: Extract
            data_path = os.getenv('DATA_PATH', '../DATA COLLECTION LAYER/Source Systems/job.csv')
            df = pd.read_csv(data_path, quotechar='"', skipinitialspace=True, encoding='utf-8', on_bad_lines='skip')
            logger.info(f"Extracted {len(df)} records from CSV")
            
            # Step 2: Transform
            df_clean = self.clean_data(df)
            logger.info("Data cleaning completed")
            
            # Step 3: Load to staging
            staging_count = self.load_to_staging(df_clean)
            
            # Step 4: Run ETL process directly (no stored procedure)
            cursor = self.connection.cursor()
            
            # Insert new time records
            try:
                time_sql = """
                INSERT IGNORE INTO DimTime (posting_date, posting_day, posting_month, posting_quarter, posting_year, posting_weekday, is_weekend)
                SELECT DISTINCT 
                    posting_date,
                    DAY(posting_date),
                    MONTH(posting_date),
                    QUARTER(posting_date),
                    YEAR(posting_date),
                    DAYNAME(posting_date),
                    DAYOFWEEK(posting_date) IN (1,7)
                FROM StagingJobPostings
                WHERE is_processed = FALSE
                """
                cursor.execute(time_sql)
                self.connection.commit()
                logger.info("Time dimension populated")
            except Error as e:
                logger.error(f"Error inserting time records: {e}")
                raise
            
            # Insert new companies
            try:
                company_sql = """
                INSERT IGNORE INTO DimCompany (company_name)
                SELECT DISTINCT company_name
                FROM StagingJobPostings
                WHERE is_processed = FALSE
                """
                cursor.execute(company_sql)
                self.connection.commit()
                logger.info("Company dimension populated")
            except Error as e:
                logger.error(f"Error inserting companies: {e}")
                raise
            
            # Insert new jobs
            try:
                job_sql = """
                INSERT IGNORE INTO DimJob (job_title, job_type, experience_level, education_level)
                SELECT DISTINCT 
                    job_title,
                    job_type,
                    experience_level,
                    education_level
                FROM StagingJobPostings
                WHERE is_processed = FALSE
                """
                cursor.execute(job_sql)
                self.connection.commit()
                logger.info("Job dimension populated")
            except Error as e:
                logger.error(f"Error inserting jobs: {e}")
                raise
            
            # Insert new industries
            try:
                industry_sql = """
                INSERT IGNORE INTO DimIndustry (industry_name)
                SELECT DISTINCT industry
                FROM StagingJobPostings
                WHERE is_processed = FALSE
                """
                cursor.execute(industry_sql)
                self.connection.commit()
                logger.info("Industry dimension populated")
            except Error as e:
                logger.error(f"Error inserting industries: {e}")
                raise
            
            # Insert new locations
            try:
                location_sql = """
                INSERT IGNORE INTO DimLocation (city)
                SELECT DISTINCT location
                FROM StagingJobPostings
                WHERE is_processed = FALSE
                """
                cursor.execute(location_sql)
                self.connection.commit()
                logger.info("Location dimension populated")
            except Error as e:
                logger.error(f"Error inserting locations: {e}")
                raise
            
            # Insert new source agencies
            try:
                agency_sql = """
                INSERT IGNORE INTO DimSourceAgency (agency_name)
                SELECT DISTINCT source_agency
                FROM StagingJobPostings
                WHERE is_processed = FALSE
                """
                cursor.execute(agency_sql)
                self.connection.commit()
                logger.info("Agency dimension populated")
            except Error as e:
                logger.error(f"Error inserting agencies: {e}")
                raise
            
            # Insert into fact table
            try:
                fact_sql = """
                INSERT INTO FactJobPosting (time_id, company_id, job_id, industry_id, location_id, source_id, salary_min, salary_max)
                SELECT 
                    t.time_id,
                    c.company_id,
                    j.job_id,
                    i.industry_id,
                    l.location_id,
                    s.source_id,
                    st.salary_min,
                    st.salary_max
                FROM StagingJobPostings st
                JOIN DimTime t ON st.posting_date = t.posting_date
                JOIN DimCompany c ON st.company_name = c.company_name
                JOIN DimJob j ON st.job_title = j.job_title 
                    AND st.job_type = j.job_type
                    AND st.experience_level = j.experience_level
                JOIN DimIndustry i ON st.industry = i.industry_name
                JOIN DimLocation l ON st.location = l.city
                JOIN DimSourceAgency s ON st.source_agency = s.agency_name
                WHERE st.is_processed = FALSE
                """
                cursor.execute(fact_sql)
                self.connection.commit()
                logger.info("Fact table populated")
            except Error as e:
                logger.error(f"Error inserting fact records: {e}")
                raise
            
            # Mark as processed
            try:
                cursor.execute("UPDATE StagingJobPostings SET is_processed = TRUE, process_date = NOW() WHERE is_processed = FALSE")
                self.connection.commit()
                logger.info("Records marked as processed")
            except Error as e:
                logger.error(f"Error updating processed status: {e}")
                raise
            
            cursor.close()
            
            logger.info("ETL pipeline completed successfully!")
            
            # Generate summary
            self.generate_summary()
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
        finally:
            if self.connection:
                self.connection.close()
                logger.info("Database connection closed")
    
    def generate_summary(self):
        """Generate summary report"""
        try:
            # Reconnect if needed
            if not self.connection or not self.connection.is_connected():
                self.setup_database_connection()
                
            cursor = self.connection.cursor(dictionary=True)
            
            queries = {
                'Total Jobs': "SELECT COUNT(*) as count FROM FactJobPosting",
                'Jobs with Salary': "SELECT COUNT(*) as count FROM FactJobPosting WHERE salary_min IS NOT NULL",
                'Unique Companies': "SELECT COUNT(DISTINCT company_id) as count FROM FactJobPosting",
                'Top Industry': """
                    SELECT i.industry_name, COUNT(*) as count 
                    FROM FactJobPosting f
                    JOIN DimIndustry i ON f.industry_id = i.industry_id
                    GROUP BY i.industry_name 
                    ORDER BY count DESC 
                    LIMIT 1
                """,
                'Top Location': """
                    SELECT l.city, COUNT(*) as count 
                    FROM FactJobPosting f
                    JOIN DimLocation l ON f.location_id = l.location_id
                    GROUP BY l.city 
                    ORDER BY count DESC 
                    LIMIT 1
                """
            }
            
            print("\n" + "="*50)
            print("ETL SUMMARY REPORT")
            print("="*50)
            for title, query in queries.items():
                cursor.execute(query)
                result = cursor.fetchone()
                if result and 'count' in result:
                    print(f"{title:20}: {result['count']}")
                elif result:
                    print(f"{title:20}: {result}")
                else:
                    print(f"{title:20}: 0")
            print("="*50)
            
            cursor.close()
            
        except Error as e:
            logger.error(f"Error generating summary: {e}")

if __name__ == "__main__":
    etl = JobETL()
    etl.run_etl_pipeline()