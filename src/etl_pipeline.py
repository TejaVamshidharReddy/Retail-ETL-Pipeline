#!/usr/bin/env python3
"""
Retail ETL Pipeline

Main ETL pipeline script for extracting, transforming, and loading retail transaction data.
This script orchestrates the entire data flow from raw sources to the target database.

Author: Teja Vamshidhar Reddy
Date: 2024
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class RetailETLPipeline:
    """
    Main ETL Pipeline class for retail data processing.
    
    This class handles the complete ETL workflow:
    - Extract: Read data from CSV files or databases
    - Transform: Clean, validate, and aggregate data
    - Load: Insert processed data into target database
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the ETL pipeline.
        
        Args:
            config: Configuration dictionary with database credentials and settings
        """
        self.config = config or self._load_config()
        self.engine = None
        self.start_time = None
        self.end_time = None
        
        # Create logs directory if it doesn't exist
        Path('logs').mkdir(exist_ok=True)
        
        logger.info("ETL Pipeline initialized")
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from environment variables or config file.
        
        Returns:
            Dictionary containing configuration parameters
        """
        return {
            'db_host': os.getenv('DB_HOST', 'localhost'),
            'db_port': os.getenv('DB_PORT', '5432'),
            'db_name': os.getenv('DB_NAME', 'retail_db'),
            'db_user': os.getenv('DB_USER', 'postgres'),
            'db_password': os.getenv('DB_PASSWORD', ''),
            'input_path': os.getenv('INPUT_PATH', 'data/'),
            'batch_size': int(os.getenv('BATCH_SIZE', '1000'))
        }
    
    def connect_database(self) -> bool:
        """
        Establish database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            connection_string = (
                f"postgresql://{self.config['db_user']}:{self.config['db_password']}"
                f"@{self.config['db_host']}:{self.config['db_port']}/{self.config['db_name']}"
            )
            self.engine = create_engine(connection_string)
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return False
    
    def extract(self, source_path: str) -> pd.DataFrame:
        """
        Extract data from source files.
        
        Args:
            source_path: Path to the source data file
        
        Returns:
            DataFrame containing raw data
        """
        logger.info(f"Starting data extraction from {source_path}")
        
        try:
            if source_path.endswith('.csv'):
                df = pd.read_csv(source_path)
            elif source_path.endswith('.xlsx'):
                df = pd.read_excel(source_path)
            else:
                raise ValueError(f"Unsupported file format: {source_path}")
            
            logger.info(f"Extracted {len(df)} records from {source_path}")
            return df
            
        except Exception as e:
            logger.error(f"Error during extraction: {str(e)}")
            raise
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and clean the raw data.
        
        Args:
            df: Raw DataFrame
        
        Returns:
            Cleaned and transformed DataFrame
        """
        logger.info("Starting data transformation")
        
        try:
            # Create a copy to avoid modifying original
            df_clean = df.copy()
            
            # Remove duplicates
            initial_count = len(df_clean)
            df_clean = df_clean.drop_duplicates()
            logger.info(f"Removed {initial_count - len(df_clean)} duplicate records")
            
            # Handle missing values
            df_clean = df_clean.dropna(subset=['transaction_id', 'date'])
            
            # Convert date column to datetime
            if 'date' in df_clean.columns:
                df_clean['date'] = pd.to_datetime(df_clean['date'])
            
            # Calculate derived fields
            if 'quantity' in df_clean.columns and 'price' in df_clean.columns:
                df_clean['total_amount'] = df_clean['quantity'] * df_clean['price']
            
            # Data type conversions
            numeric_columns = ['quantity', 'price']
            for col in numeric_columns:
                if col in df_clean.columns:
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            
            # Remove invalid records (negative quantities or prices)
            if 'quantity' in df_clean.columns:
                df_clean = df_clean[df_clean['quantity'] > 0]
            if 'price' in df_clean.columns:
                df_clean = df_clean[df_clean['price'] > 0]
            
            logger.info(f"Transformation complete. {len(df_clean)} records ready for loading")
            return df_clean
            
        except Exception as e:
            logger.error(f"Error during transformation: {str(e)}")
            raise
    
    def load(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append') -> bool:
        """
        Load transformed data into the database.
        
        Args:
            df: Cleaned DataFrame to load
            table_name: Target table name
            if_exists: How to behave if table exists ('append', 'replace', 'fail')
        
        Returns:
            True if load successful, False otherwise
        """
        logger.info(f"Starting data load to table '{table_name}'")
        
        try:
            if self.engine is None:
                raise RuntimeError("Database connection not established")
            
            # Load data in batches
            batch_size = self.config['batch_size']
            total_batches = len(df) // batch_size + (1 if len(df) % batch_size else 0)
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                batch.to_sql(
                    table_name,
                    self.engine,
                    if_exists=if_exists if i == 0 else 'append',
                    index=False
                )
                batch_num = i // batch_size + 1
                logger.info(f"Loaded batch {batch_num}/{total_batches}")
            
            logger.info(f"Successfully loaded {len(df)} records to '{table_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Error during loading: {str(e)}")
            return False
    
    def run(self, source_path: str, target_table: str = 'fact_transactions') -> bool:
        """
        Execute the complete ETL pipeline.
        
        Args:
            source_path: Path to source data file
            target_table: Name of target database table
        
        Returns:
            True if pipeline executed successfully, False otherwise
        """
        self.start_time = datetime.now()
        logger.info("=" * 50)
        logger.info("Starting ETL Pipeline Execution")
        logger.info(f"Source: {source_path}")
        logger.info(f"Target: {target_table}")
        logger.info("=" * 50)
        
        try:
            # Step 1: Connect to database
            if not self.connect_database():
                return False
            
            # Step 2: Extract
            raw_data = self.extract(source_path)
            
            # Step 3: Transform
            clean_data = self.transform(raw_data)
            
            # Step 4: Load
            success = self.load(clean_data, target_table)
            
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            if success:
                logger.info("=" * 50)
                logger.info("ETL Pipeline Completed Successfully")
                logger.info(f"Duration: {duration:.2f} seconds")
                logger.info(f"Records processed: {len(clean_data)}")
                logger.info("=" * 50)
            
            return success
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return False
    
    def validate_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform data quality validation.
        
        Args:
            df: DataFrame to validate
        
        Returns:
            Dictionary containing validation results
        """
        validation_results = {
            'total_records': len(df),
            'missing_values': df.isnull().sum().to_dict(),
            'duplicate_records': df.duplicated().sum(),
            'columns': list(df.columns),
            'data_types': df.dtypes.to_dict()
        }
        
        return validation_results


def main():
    """
    Main entry point for the ETL pipeline.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Retail ETL Pipeline')
    parser.add_argument(
        '--source',
        type=str,
        default='data/sample_transactions.csv',
        help='Path to source data file'
    )
    parser.add_argument(
        '--table',
        type=str,
        default='fact_transactions',
        help='Target database table name'
    )
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Run validation only without loading data'
    )
    
    args = parser.parse_args()
    
    # Initialize and run pipeline
    pipeline = RetailETLPipeline()
    
    if args.validate_only:
        logger.info("Running in validation-only mode")
        raw_data = pipeline.extract(args.source)
        validation_results = pipeline.validate_data(raw_data)
        logger.info(f"Validation Results: {validation_results}")
    else:
        success = pipeline.run(args.source, args.table)
        sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
