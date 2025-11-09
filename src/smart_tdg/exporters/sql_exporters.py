"""SQL statement exporters."""

from pathlib import Path
from typing import Dict, List
import pandas as pd
from utils.config import Config


class SQLExporter:
    """Export generated data as SQL statements."""
    
    def __init__(self, generated_data: Dict[str, pd.DataFrame]):
        """
        Initialize SQL exporter.
        
        Args:
            generated_data: Dictionary mapping table names to DataFrames
        """
        self.generated_data = generated_data
    
    def export_insert_statements(
        self, 
        output_file: str = None, 
        dialect: str = 'postgresql',
        batch_size: int = 100
    ):
        """
        Export as SQL INSERT statements.
        
        Args:
            output_file: Output SQL file path
            dialect: SQL dialect ('postgresql', 'mysql', 'sqlite')
            batch_size: Number of rows per INSERT statement
        """
        output_file = Path(output_file or Config.OUTPUT_DIR / "insert_statements.sql")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            # Write header
            f.write(f"-- Generated SQL INSERT Statements\n")
            f.write(f"-- Dialect: {dialect}\n")
            f.write(f"-- Total tables: {len(self.generated_data)}\n\n")
            
            for table_name, df in self.generated_data.items():
                f.write(f"-- Table: {table_name} ({len(df)} rows)\n")
                f.write(f"-- " + "=" * 60 + "\n\n")
                
                # Generate INSERT statements
                columns = ', '.join(df.columns)
                
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    
                    if dialect == 'postgresql':
                        self._write_postgresql_insert(f, table_name, columns, batch)
                    elif dialect == 'mysql':
                        self._write_mysql_insert(f, table_name, columns, batch)
                    else:  # sqlite
                        self._write_sqlite_insert(f, table_name, columns, batch)
                
                f.write("\n")
        
        total_rows = sum(len(df) for df in self.generated_data.values())
        print(f"✓ Exported SQL INSERT statements to {output_file} ({total_rows} rows)")
    
    def _write_postgresql_insert(self, f, table_name: str, columns: str, batch: pd.DataFrame):
        """Write PostgreSQL INSERT statement."""
        f.write(f"INSERT INTO {table_name} ({columns}) VALUES\n")
        
        values_list = []
        for _, row in batch.iterrows():
            values = ', '.join(self._format_value(v) for v in row.values)
            values_list.append(f"  ({values})")
        
        f.write(',\n'.join(values_list))
        f.write(";\n\n")
    
    def _write_mysql_insert(self, f, table_name: str, columns: str, batch: pd.DataFrame):
        """Write MySQL INSERT statement."""
        f.write(f"INSERT INTO {table_name} ({columns}) VALUES\n")
        
        values_list = []
        for _, row in batch.iterrows():
            values = ', '.join(self._format_value(v) for v in row.values)
            values_list.append(f"  ({values})")
        
        f.write(',\n'.join(values_list))
        f.write(";\n\n")
    
    def _write_sqlite_insert(self, f, table_name: str, columns: str, batch: pd.DataFrame):
        """Write SQLite INSERT statement."""
        for _, row in batch.iterrows():
            values = ', '.join(self._format_value(v) for v in row.values)
            f.write(f"INSERT INTO {table_name} ({columns}) VALUES ({values});\n")
        f.write("\n")
    
    def _format_value(self, value) -> str:
        """Format a value for SQL."""
        if pd.isna(value):
            return 'NULL'
        elif isinstance(value, str):
            # Escape single quotes
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        else:
            # Convert to string and quote
            return f"'{str(value)}'"
    
    def export_copy_statements(self, output_file: str = None):
        """
        Export as PostgreSQL COPY statements (faster than INSERT).
        
        Args:
            output_file: Output SQL file path
        """
        output_file = Path(output_file or Config.OUTPUT_DIR / "copy_statements.sql")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            f.write("-- PostgreSQL COPY Statements\n\n")
            
            for table_name, df in self.generated_data.items():
                csv_file = f"{table_name}.csv"
                columns = ', '.join(df.columns)
                
                f.write(f"\\COPY {table_name} ({columns}) FROM '{csv_file}' WITH (FORMAT CSV, HEADER TRUE);\n")
        
        print(f"✓ Exported COPY statements to {output_file}")
        print("  Note: Export CSV files separately and place them in the same directory")
