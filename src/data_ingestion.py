"""
Data Ingestion Module for Bronze Layer
Loads raw CSV files, validates them, and converts to Parquet format
"""

import pandas as pd
import yaml
import logging
import logging.config
from pathlib import Path
import sys

# Setup logging
logging.config.fileConfig('config/logging.conf')
logger = logging.getLogger('dataIngestion')


class DataIngestion:
    """
    Handles data ingestion from raw CSV to Bronze layer (Parquet format)
    """
    
    def __init__(self, config_path='config/settings.yaml'):
        """Initialize with configuration file"""
        logger.info("Initializing DataIngestion module")
        
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Setup paths
        self.raw_path = Path(self.config['paths']['raw_data'])
        self.bronze_path = Path(self.config['paths']['bronze_layer'])
        
        # Create bronze directory if it doesn't exist
        self.bronze_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Bronze layer directory: {self.bronze_path}")
        
        # Data files mapping
        self.data_files = self.config['data_files']
        self.validation_rules = self.config['validation']['min_rows']
        
        logger.info("DataIngestion initialized successfully")
    
    
    def validate_data(self, df, table_name):
        """
        Validate dataframe for basic quality checks
        
        Args:
            df: pandas DataFrame
            table_name: name of the table
            
        Returns:
            bool: True if validation passes
        """
        logger.info(f"Validating {table_name}...")
        
        # Check row count
        min_rows = self.validation_rules.get(table_name, 0)
        actual_rows = len(df)
        
        if actual_rows < min_rows:
            logger.warning(f"{table_name}: Expected at least {min_rows} rows, got {actual_rows}")
            return False
        
        logger.info(f"{table_name}: Row count OK ({actual_rows:,} rows)")
        
        # Check for completely empty dataframe
        if df.empty:
            logger.error(f"{table_name}: DataFrame is empty!")
            return False
        
        # Log missing values
        missing_counts = df.isnull().sum()
        if missing_counts.sum() > 0:
            logger.warning(f"{table_name}: Missing values detected:\n{missing_counts[missing_counts > 0]}")
        
        # Log memory usage
        memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
        logger.info(f"{table_name}: Memory usage: {memory_mb:.2f} MB")
        
        return True
    
    
    def load_csv(self, file_name, table_name):
        """
        Load CSV file with error handling
        
        Args:
            file_name: CSV file name
            table_name: logical name of the table
            
        Returns:
            pd.DataFrame or None if failed
        """
        file_path = self.raw_path / file_name
        
        try:
            logger.info(f"Loading {file_name}...")
            df = pd.read_csv(file_path)
            logger.info(f"Successfully loaded {file_name}: {df.shape}")
            return df
            
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return None
        except pd.errors.EmptyDataError:
            logger.error(f"File is empty: {file_path}")
            return None
        except Exception as e:
            logger.error(f"Error loading {file_path}: {str(e)}")
            return None
    
    
    def save_to_parquet(self, df, table_name):
        """
        Save DataFrame to Parquet format
        
        Args:
            df: pandas DataFrame
            table_name: name for the output file
        """
        output_path = self.bronze_path / f"{table_name}.parquet"
        
        try:
            df.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
            logger.info(f"Saved {table_name} to {output_path}")
            
            # Log file size
            file_size_mb = output_path.stat().st_size / 1024 / 1024
            logger.info(f"{table_name}.parquet size: {file_size_mb:.2f} MB")
            
        except Exception as e:
            logger.error(f"Error saving {table_name} to Parquet: {str(e)}")
            raise
    
    
    def ingest_all(self):
        """
        Main method to ingest all data files
        """
        logger.info("="*60)
        logger.info("Starting Bronze Layer Ingestion")
        logger.info("="*60)
        
        success_count = 0
        fail_count = 0
        
        for table_name, file_name in self.data_files.items():
            logger.info(f"\nProcessing {table_name}...")
            
            # Load CSV
            df = self.load_csv(file_name, table_name)
            if df is None:
                fail_count += 1
                continue
            
            # Validate
            if not self.validate_data(df, table_name):
                logger.warning(f"{table_name}: Validation failed, but continuing...")
            
            # Save to Parquet
            try:
                self.save_to_parquet(df, table_name)
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to save {table_name}: {str(e)}")
                fail_count += 1
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("Bronze Layer Ingestion Complete")
        logger.info(f"Success: {success_count}/{len(self.data_files)}")
        logger.info(f"Failed: {fail_count}/{len(self.data_files)}")
        logger.info("="*60)
        
        return success_count, fail_count


def main():
    """Run the ingestion pipeline"""
    try:
        ingestion = DataIngestion()
        success, failed = ingestion.ingest_all()
        
        if failed > 0:
            logger.warning(f"Ingestion completed with {failed} failures")
            sys.exit(1)
        else:
            logger.info("All files ingested successfully!")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Fatal error in ingestion pipeline: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
