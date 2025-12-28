"""
Snowflake database setup and data loading script.
Uses snowflake-connector-python and Snowpark for data processing.
"""
import os
import sys
from pathlib import Path
import snowflake.connector
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sum_, avg, count, month, year, lag
from snowflake.snowpark import Window


class SnowflakeManager:
    """Manages Snowflake database operations."""
    
    def __init__(self):
        """Initialize Snowflake connection parameters."""
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.password = os.getenv("SNOWFLAKE_PASSWORD")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        self.database = os.getenv("SNOWFLAKE_DATABASE", "SAAS_ANALYTICS")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
        self.role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
        
        self.data_dir = Path(__file__).parent.parent / "data"
        self.conn = None
        self.session = None
    
    def connect(self):
        """Establish connection to Snowflake."""
        print("Connecting to Snowflake...")
        try:
            self.conn = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                role=self.role
            )
            print("✓ Connected to Snowflake successfully")
            return True
        except Exception as e:
            print(f"✗ Error connecting to Snowflake: {e}")
            return False
    
    def create_snowpark_session(self):
        """Create Snowpark session for data processing."""
        print("Creating Snowpark session...")
        try:
            connection_parameters = {
                "account": self.account,
                "user": self.user,
                "password": self.password,
                "role": self.role,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema
            }
            self.session = Session.builder.configs(connection_parameters).create()
            print("✓ Snowpark session created successfully")
            return True
        except Exception as e:
            print(f"✗ Error creating Snowpark session: {e}")
            return False
    
    def execute_query(self, query, fetch=False):
        """Execute a SQL query."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            if fetch:
                return cursor.fetchall()
            return True
        except Exception as e:
            print(f"✗ Error executing query: {e}")
            print(f"Query: {query[:100]}...")
            return None
    
    def setup_database(self):
        """Create database and schema."""
        print("\n" + "="*50)
        print("Setting up database and schema...")
        print("="*50)
        
        queries = [
            f"CREATE DATABASE IF NOT EXISTS {self.database}",
            f"USE DATABASE {self.database}",
            f"CREATE SCHEMA IF NOT EXISTS {self.schema}",
            f"USE SCHEMA {self.schema}"
        ]
        
        for query in queries:
            print(f"Executing: {query}")
            if self.execute_query(query):
                print("✓ Success")
        
        print("\n✓ Database and schema setup complete")
    
    def create_stage(self):
        """Create stage for file upload."""
        print("\n" + "="*50)
        print("Creating stage for CSV files...")
        print("="*50)
        
        query = f"""
        CREATE OR REPLACE STAGE {self.database}.{self.schema}.saas_data_stage
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        """
        
        if self.execute_query(query):
            print("✓ Stage created successfully")
            return True
        return False
    
    def create_tables(self):
        """Create tables for customers, credit_usage, and billing."""
        print("\n" + "="*50)
        print("Creating tables...")
        print("="*50)
        
        # Customers table
        customers_table = f"""
        CREATE OR REPLACE TABLE {self.database}.{self.schema}.customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255),
            signup_date DATE,
            tier VARCHAR(50),
            status VARCHAR(50),
            industry VARCHAR(100),
            region VARCHAR(100)
        )
        """
        
        # Credit usage table
        credit_usage_table = f"""
        CREATE OR REPLACE TABLE {self.database}.{self.schema}.credit_usage (
            usage_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            date DATE,
            credits_used DECIMAL(18,2),
            feature VARCHAR(100),
            execution_time_ms INTEGER,
            FOREIGN KEY (customer_id) REFERENCES {self.database}.{self.schema}.customers(customer_id)
        )
        """
        
        # Billing table
        billing_table = f"""
        CREATE OR REPLACE TABLE {self.database}.{self.schema}.billing (
            billing_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            billing_date DATE,
            amount DECIMAL(18,2),
            period VARCHAR(10),
            status VARCHAR(50),
            currency VARCHAR(10),
            FOREIGN KEY (customer_id) REFERENCES {self.database}.{self.schema}.customers(customer_id)
        )
        """
        
        tables = [
            ("customers", customers_table),
            ("credit_usage", credit_usage_table),
            ("billing", billing_table)
        ]
        
        for table_name, query in tables:
            print(f"\nCreating {table_name} table...")
            if self.execute_query(query):
                print(f"✓ {table_name} table created successfully")
        
        print("\n✓ All tables created successfully")
    
    def upload_and_load_data(self):
        """Upload CSV files to stage and load into tables."""
        print("\n" + "="*50)
        print("Uploading and loading data...")
        print("="*50)
        
        # Check if data files exist
        csv_files = ["customers.csv", "credit_usage.csv", "billing.csv"]
        for csv_file in csv_files:
            file_path = self.data_dir / csv_file
            if not file_path.exists():
                print(f"✗ Data file not found: {file_path}")
                return False
        
        # Upload files to stage and load into tables
        for csv_file in csv_files:
            table_name = csv_file.replace(".csv", "")
            file_path = self.data_dir / csv_file
            
            print(f"\n--- Processing {table_name} ---")
            
            # Upload to stage
            print(f"Uploading {csv_file} to stage...")
            put_query = f"PUT file://{file_path} @{self.database}.{self.schema}.saas_data_stage OVERWRITE=TRUE"
            if self.execute_query(put_query):
                print(f"✓ {csv_file} uploaded successfully")
            
            # Load into table
            print(f"Loading data into {table_name} table...")
            copy_query = f"""
            COPY INTO {self.database}.{self.schema}.{table_name}
            FROM @{self.database}.{self.schema}.saas_data_stage/{csv_file}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
            """
            if self.execute_query(copy_query):
                print(f"✓ Data loaded into {table_name} table successfully")
        
        print("\n✓ All data uploaded and loaded successfully")
    
    def process_with_snowpark(self):
        """Process data using Snowpark for analytics."""
        print("\n" + "="*50)
        print("Processing data with Snowpark...")
        print("="*50)
        
        if not self.session:
            print("✗ Snowpark session not available")
            return False
        
        try:
            # Read tables using Snowpark
            print("\nReading tables with Snowpark...")
            customers_df = self.session.table(f"{self.database}.{self.schema}.customers")
            credit_usage_df = self.session.table(f"{self.database}.{self.schema}.credit_usage")
            billing_df = self.session.table(f"{self.database}.{self.schema}.billing")
            
            # Example: Calculate customer metrics
            print("\nCalculating customer metrics...")
            
            # Total credits used per customer
            customer_credits = credit_usage_df.group_by("customer_id") \
                .agg(sum_("credits_used").alias("total_credits_used"),
                     count("usage_id").alias("usage_count"))
            
            # Join with customer data
            customer_metrics = customers_df.join(customer_credits, "customer_id", "left")
            
            # Show sample results
            print("\nSample customer metrics:")
            customer_metrics.show(5)
            
            # Create a view for customer health scores
            print("\nCreating customer health score view...")
            create_view_query = f"""
            CREATE OR REPLACE VIEW {self.database}.{self.schema}.customer_health_scores AS
            SELECT 
                c.customer_id,
                c.name,
                c.tier,
                c.status,
                COALESCE(SUM(cu.credits_used), 0) as total_credits,
                COALESCE(COUNT(DISTINCT cu.date), 0) as active_days,
                COALESCE(AVG(cu.credits_used), 0) as avg_credits_per_day,
                COALESCE(SUM(b.amount), 0) as total_revenue
            FROM {self.database}.{self.schema}.customers c
            LEFT JOIN {self.database}.{self.schema}.credit_usage cu ON c.customer_id = cu.customer_id
            LEFT JOIN {self.database}.{self.schema}.billing b ON c.customer_id = b.customer_id
            GROUP BY c.customer_id, c.name, c.tier, c.status
            """
            
            if self.execute_query(create_view_query):
                print("✓ Customer health score view created successfully")
            
            print("\n✓ Snowpark processing complete")
            return True
            
        except Exception as e:
            print(f"✗ Error in Snowpark processing: {e}")
            return False
    
    def verify_data(self):
        """Verify data was loaded correctly."""
        print("\n" + "="*50)
        print("Verifying data...")
        print("="*50)
        
        tables = ["customers", "credit_usage", "billing"]
        
        for table in tables:
            query = f"SELECT COUNT(*) FROM {self.database}.{self.schema}.{table}"
            result = self.execute_query(query, fetch=True)
            if result:
                count = result[0][0]
                print(f"✓ {table}: {count} records")
        
        print("\n✓ Data verification complete")
    
    def close(self):
        """Close connections."""
        if self.conn:
            self.conn.close()
            print("\n✓ Snowflake connection closed")
        if self.session:
            self.session.close()
            print("✓ Snowpark session closed")


def main():
    """Main function to setup Snowflake and load data."""
    print("\n" + "="*70)
    print(" " * 15 + "SNOWFLAKE SAAS ANALYTICS SETUP")
    print("="*70 + "\n")
    
    # Check if credentials are set
    required_env_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        print("⚠ Warning: Missing Snowflake credentials!")
        print(f"Missing environment variables: {', '.join(missing_vars)}")
        print("\nPlease set the following environment variables:")
        print("  - SNOWFLAKE_ACCOUNT")
        print("  - SNOWFLAKE_USER")
        print("  - SNOWFLAKE_PASSWORD")
        print("  - SNOWFLAKE_WAREHOUSE (optional, default: COMPUTE_WH)")
        print("  - SNOWFLAKE_DATABASE (optional, default: SAAS_ANALYTICS)")
        print("  - SNOWFLAKE_SCHEMA (optional, default: PUBLIC)")
        print("  - SNOWFLAKE_ROLE (optional, default: ACCOUNTADMIN)")
        print("\nExample:")
        print("  export SNOWFLAKE_ACCOUNT=your_account")
        print("  export SNOWFLAKE_USER=your_username")
        print("  export SNOWFLAKE_PASSWORD=your_password")
        print("\nRunning in demo mode (will show structure but skip actual Snowflake operations)")
        return
    
    # Initialize Snowflake manager
    manager = SnowflakeManager()
    
    # Connect to Snowflake
    if not manager.connect():
        print("\n✗ Failed to connect to Snowflake")
        sys.exit(1)
    
    try:
        # Setup database and schema
        manager.setup_database()
        
        # Create stage
        manager.create_stage()
        
        # Create tables
        manager.create_tables()
        
        # Upload and load data
        manager.upload_and_load_data()
        
        # Verify data
        manager.verify_data()
        
        # Create Snowpark session and process data
        if manager.create_snowpark_session():
            manager.process_with_snowpark()
        
        print("\n" + "="*70)
        print(" " * 20 + "SETUP COMPLETE!")
        print("="*70)
        print("\nYour Snowflake SaaS analytics database is ready to use.")
        print(f"Database: {manager.database}")
        print(f"Schema: {manager.schema}")
        print(f"Tables: customers, credit_usage, billing")
        print(f"View: customer_health_scores")
        
    except Exception as e:
        print(f"\n✗ Error during setup: {e}")
        sys.exit(1)
    finally:
        manager.close()


if __name__ == "__main__":
    main()
