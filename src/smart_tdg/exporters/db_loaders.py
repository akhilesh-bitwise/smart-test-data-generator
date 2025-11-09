"""Database loaders for direct data insertion."""

from typing import Dict, Optional
import pandas as pd
from sqlalchemy import create_engine
from smart_tdg.utils.config import Config


class DatabaseLoader:
    """Load generated data directly into databases."""
    
    def __init__(self, generated_data: Dict[str, pd.DataFrame]):
        """
        Initialize database loader.
        
        Args:
            generated_data: Dictionary mapping table names to DataFrames
        """
        self.generated_data = generated_data
    
    def load_to_postgres(
        self, 
        connection_string: Optional[str] = None,
        if_exists: str = 'replace',
        chunksize: Optional[int] = None
    ):
        """
        Load data to PostgreSQL.
        
        Args:
            connection_string: PostgreSQL connection string
            if_exists: What to do if table exists ('fail', 'replace', 'append')
            chunksize: Number of rows per batch
        """
        connection_string = connection_string or Config.POSTGRES_URI
        
        if not connection_string:
            raise ValueError(
                "PostgreSQL connection string not provided. "
                "Set POSTGRES_URI in .env file"
            )
        
        print(f"\nConnecting to PostgreSQL...")
        print(f"  Connection: {connection_string.split('@')[1] if '@' in connection_string else 'localhost'}")
        
        try:
            engine = create_engine(connection_string)
            
            # Test connection
            with engine.connect() as conn:
                print(f"  ✓ Connection successful")
            
            for table_name, df in self.generated_data.items():
                print(f"\nLoading table: {table_name}")
                print(f"  Rows: {len(df)}")
                print(f"  Columns: {list(df.columns)}")
                
                df.to_sql(
                    table_name, 
                    engine, 
                    if_exists=if_exists, 
                    index=False,
                    chunksize=chunksize or 1000
                )
                print(f"  ✓ Loaded {len(df)} rows to PostgreSQL table: {table_name}")
            
            engine.dispose()
            print(f"\n✓ All tables loaded successfully")
            
        except Exception as e:
            print(f"\n✗ Error loading to PostgreSQL: {e}")
            raise
    
    def verify_data(self, connection_string: Optional[str] = None):
        """
        Verify data was loaded correctly.
        
        Args:
            connection_string: PostgreSQL connection string
        """
        connection_string = connection_string or Config.POSTGRES_URI
        
        if not connection_string:
            raise ValueError("PostgreSQL connection string not provided")
        
        engine = create_engine(connection_string)
        
        print("\nVerifying data in PostgreSQL:")
        print("=" * 60)
        
        for table_name in self.generated_data.keys():
            try:
                query = f"SELECT COUNT(*) as count FROM {table_name}"
                result = pd.read_sql(query, engine)
                count = result['count'][0]
                expected = len(self.generated_data[table_name])
                
                status = "✓" if count == expected else "✗"
                print(f"{status} {table_name}: {count} rows (expected {expected})")
                
            except Exception as e:
                print(f"✗ {table_name}: Error - {e}")
        
        engine.dispose()
