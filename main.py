import pandas as pd
import mysql.connector
from sqlalchemy import create_engine, URL, text
import os
import logging
from typing import List, Dict, Any
from datetime import datetime
from urllib.parse import quote_plus
import re
import time

# Set up logging at the beginning of the script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MySQLTestSetup:
    """Handles MySQL database and test data setup"""
    def __init__(self, mysql_config: Dict[str, str]):
        self.mysql_config = mysql_config
        self.logger = logger  # Use the global logger
        
    def create_database(self):
        """Create test database if it doesn't exist"""
        config = self.mysql_config.copy()
        config.pop('database', None)
        
        try:
            conn = mysql.connector.connect(
                host=config['host'],
                user=config['user'],
                password=config['password']
            )
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.mysql_config['database']}")
            cursor.close()
            conn.close()
            self.logger.info(f"Database {self.mysql_config['database']} created successfully")
        except Exception as e:
            self.logger.error(f"Error creating database: {str(e)}")
            raise
        
    def create_generic_table(self):
        """Create a generic table to store data from all files"""
        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS all_data (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    source_file VARCHAR(255) NOT NULL,
                    data_column VARCHAR(255) NOT NULL,
                    data_value TEXT,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.close()
            conn.close()
            self.logger.info("Generic table 'all_data' created successfully")
        except Exception as e:
            self.logger.error(f"Error creating generic table: {str(e)}")
            raise

    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str):
        """Create a table based on DataFrame schema if it doesn't exist"""
        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            # Sanitize the table name
            table_name = self.sanitize_table_name(table_name)
            
            # Generate CREATE TABLE statement
            create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ("
            columns = []
            for column, dtype in df.dtypes.items():
                sql_type = self.pandas_dtype_to_mysql_type(dtype)
                column_name = self.sanitize_column_name(column)
                columns.append(f"`{column_name}` {sql_type}")
            create_table_sql += ", ".join(columns) + ")"
            
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()
            self.logger.info(f"Table {table_name} created successfully")
        except Exception as e:
            self.logger.error(f"Error creating table {table_name}: {str(e)}")
            raise

    def pandas_dtype_to_mysql_type(self, dtype):
        """Convert pandas dtype to MySQL column type"""
        if pd.api.types.is_integer_dtype(dtype):
            return "INT"
        elif pd.api.types.is_float_dtype(dtype):
            return "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATETIME"
        elif pd.api.types.is_string_dtype(dtype):
            return "VARCHAR(255)"
        else:
            return "TEXT"

    def sanitize_table_name(self, table_name: str) -> str:
        """Sanitize the table name to be valid in MySQL"""
        # Remove all non-alphanumeric characters except underscores
        sanitized = re.sub(r'[^\w]', '', table_name)
        # Remove leading digits
        sanitized = re.sub(r'^\d+', '', sanitized)
        # If the name is empty after sanitization, use a default name
        if not sanitized:
            sanitized = 'table'
        # Truncate to 63 characters (MySQL's limit is 64, but we'll add a prefix)
        sanitized = sanitized[:63]
        # Ensure the table name is unique by adding a prefix
        sanitized = f"t_{sanitized}"
        self.logger.info(f"Sanitized table name: {table_name} -> {sanitized}")
        return sanitized

    def sanitize_column_name(self, column_name: str) -> str:
        """Sanitize the column name to be valid in MySQL"""
        # Replace non-alphanumeric characters with underscores
        sanitized = re.sub(r'[^\w]', '_', column_name)
        # Ensure the column name starts with a letter
        if not sanitized[0].isalpha():
            sanitized = 'column_' + sanitized
        # Truncate to 64 characters (MySQL's limit)
        return sanitized[:64]

class DataFileProcessor:
    def __init__(self, mysql_config: Dict[str, str], table_name: str):
        self.mysql_config = mysql_config
        self.table_name = table_name
        self.logger = logger  # Use the global logger
    
    def get_database_url(self) -> str:
        """Create properly formatted database URL"""
        password = quote_plus(self.mysql_config['password'])
        return f"mysql+mysqlconnector://{self.mysql_config['user']}:{password}@{self.mysql_config['host']}/{self.mysql_config['database']}"
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            engine = create_engine(self.get_database_url())
            with engine.connect() as connection:
                self.logger.info("Database connection test successful")
                return True
        except Exception as e:
            self.logger.error(f"Database connection test failed: {str(e)}")
            return False

    def process_directory(self, directory_path: str) -> Dict[str, pd.DataFrame]:
        self.logger.info(f"Processing directory: {directory_path}")
        processed_dfs = {}
        
        if not os.path.exists(directory_path):
            self.logger.error(f"Directory not found: {directory_path}")
            return processed_dfs
        
        for filename in os.listdir(directory_path):
            if filename.endswith(('.xlsx', '.xls', '.csv')):
                file_path = os.path.join(directory_path, filename)
                try:
                    df = self.process_file(file_path)
                    if df is not None:
                        table_name = os.path.splitext(filename)[0]
                        table_name = MySQLTestSetup(self.mysql_config).sanitize_table_name(table_name)
                        processed_dfs[table_name] = df
                        self.logger.info(f"Successfully processed {filename}")
                except Exception as e:
                    self.logger.error(f"Error processing {filename}: {str(e)}")
                    continue
        
        return processed_dfs
    
    def process_file(self, file_path: str) -> pd.DataFrame:
        try:
            file_ext = os.path.splitext(file_path)[1].lower()
            if file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            elif file_ext == '.csv':
                df = pd.read_csv(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
            
            # Infer and convert data types
            for column in df.columns:
                df[column] = self.infer_and_convert_type(df[column])
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {str(e)}")
            raise
    
    def infer_and_convert_type(self, series: pd.Series) -> pd.Series:
        if series.dtype == 'object':
            # Try to convert to datetime
            try:
                return pd.to_datetime(series)
            except ValueError:
                pass
            
            # Try to convert to numeric
            try:
                return pd.to_numeric(series)
            except ValueError:
                pass
            
            # If it's mostly boolean values, convert to boolean
            if series.isin([True, False, 'True', 'False', 1, 0]).mean() > 0.8:
                return series.map({'True': True, 'False': False, '1': True, '0': False})
        
        return series

    def save_to_database(self, df: pd.DataFrame, file_name: str):
        try:
            if not self.test_connection():
                raise Exception("Database connection test failed")
            
            engine = create_engine(self.get_database_url())
            
            # Melt the dataframe to convert columns to rows
            melted_df = df.melt(var_name='data_column', value_name='data_value')
            melted_df['source_file'] = file_name

            # Save data to the table
            self.logger.info(f"Saving data from file: {file_name}")
            melted_df.to_sql(
                name='all_data',
                con=engine,
                if_exists='append',
                index=False
            )
            
            self.logger.info(f"Successfully saved {len(df)} records from {file_name}")
            
        except Exception as e:
            self.logger.error(f"Error saving data from file {file_name}: {str(e)}")
            raise

def main():
    # MySQL configuration
    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'Admin@123',
        'database': 'employee_test_db'
    }
    
    try:
        # Test MySQL configuration
        test_conn = mysql.connector.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password']
        )
        test_conn.close()
        
        # Setup test environment
        logger.info("Setting up test environment...")
        setup = MySQLTestSetup(mysql_config)
        setup.create_database()
        setup.create_generic_table()
        
        # Initialize processor
        processor = DataFileProcessor(mysql_config, '')
        
        # Directory to monitor
        data_dir = "/Users/shivraj/Downloads/data/"
        
        # Keep track of processed files
        processed_files = set()
        
        logger.info(f"Starting to monitor directory: {data_dir}")
        
        while True:
            # Get list of files in the directory
            current_files = set(os.listdir(data_dir))
            
            # Find new files
            new_files = current_files - processed_files
            
            if new_files:
                logger.info(f"Found {len(new_files)} new file(s) to process.")
                
                for filename in new_files:
                    if filename.endswith(('.xlsx', '.xls', '.csv')):
                        file_path = os.path.join(data_dir, filename)
                        try:
                            df = processor.process_file(file_path)
                            if df is not None:
                                processor.save_to_database(df, filename)
                                logger.info(f"Successfully processed and saved {filename}")
                        except Exception as e:
                            logger.error(f"Error processing {filename}: {str(e)}")
                        
                        # Mark file as processed
                        processed_files.add(filename)
            
            else:
                logger.info("No new files to process.")
            
            # Wait for a specified interval before checking again
            time.sleep(60)  # Check every 60 seconds
            
    except KeyboardInterrupt:
        logger.info("Program stopped by user.")
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()
