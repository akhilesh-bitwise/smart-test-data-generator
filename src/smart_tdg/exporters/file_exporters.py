"""File-based exporters for generated data."""

import json
from pathlib import Path
from typing import Dict
import pandas as pd
from utils.config import Config


class FileExporter:
    """Export generated data to various file formats."""
    
    def __init__(self, generated_data: Dict[str, pd.DataFrame]):
        """
        Initialize file exporter.
        
        Args:
            generated_data: Dictionary mapping table names to DataFrames
        """
        self.generated_data = generated_data
    
    def export(self, format: str = 'csv', **kwargs):
        """
        Export data in specified format.
        
        Args:
            format: Export format ('csv', 'json', 'parquet')
            **kwargs: Format-specific options
        """
        if format == 'csv':
            self.export_csv(**kwargs)
        elif format == 'json':
            self.export_json(**kwargs)
        elif format == 'parquet':
            self.export_parquet(**kwargs)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def export_csv(self, output_dir: str = None, index: bool = False):
        """
        Export to CSV files.
        
        Args:
            output_dir: Output directory path
            index: Whether to include DataFrame index
        """
        output_dir = Path(output_dir or Config.OUTPUT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for table_name, df in self.generated_data.items():
            filepath = output_dir / f"{table_name}.csv"
            df.to_csv(filepath, index=index)
            print(f"✓ Exported {table_name} to {filepath} ({len(df)} rows)")
    
    def export_json(self, output_file: str = None, orient: str = 'records', indent: int = 2):
        """
        Export to JSON file.
        
        Args:
            output_file: Output file path
            orient: JSON orientation ('records', 'table', 'index')
            indent: JSON indentation level
        """
        output_file = Path(output_file or Config.OUTPUT_DIR / "generated_data.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert DataFrames to JSON-serializable format
        data_dict = {
            table_name: df.to_dict(orient=orient)
            for table_name, df in self.generated_data.items()
        }
        
        with open(output_file, 'w') as f:
            json.dump(data_dict, f, indent=indent, default=str)
        
        total_rows = sum(len(df) for df in self.generated_data.values())
        print(f"✓ Exported {len(data_dict)} tables to {output_file} ({total_rows} total rows)")
    
    def export_parquet(self, output_dir: str = None, compression: str = 'snappy'):
        """
        Export to Parquet files.
        
        Args:
            output_dir: Output directory path
            compression: Compression algorithm ('snappy', 'gzip', 'brotli')
        """
        output_dir = Path(output_dir or Config.OUTPUT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for table_name, df in self.generated_data.items():
            filepath = output_dir / f"{table_name}.parquet"
            df.to_parquet(filepath, compression=compression, index=False)
            file_size = filepath.stat().st_size / 1024  # KB
            print(f"✓ Exported {table_name} to {filepath} ({len(df)} rows, {file_size:.2f} KB)")
    
    def export_separate_files(self, output_dir: str = None, format: str = 'csv'):
        """
        Export each table to a separate file.
        
        Args:
            output_dir: Output directory path
            format: File format ('csv', 'json', 'parquet')
        """
        if format == 'csv':
            self.export_csv(output_dir)
        elif format == 'json':
            output_dir = Path(output_dir or Config.OUTPUT_DIR)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            for table_name, df in self.generated_data.items():
                filepath = output_dir / f"{table_name}.json"
                df.to_json(filepath, orient='records', indent=2)
                print(f"✓ Exported {table_name} to {filepath}")
        elif format == 'parquet':
            self.export_parquet(output_dir)

    def export_quality_report(self, quality_reporter, output_dir: str = None):
        """Export quality report JSON and HTML."""
        output_dir = Path(output_dir or "./output/")
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / "quality_report.json"
        html_path = output_dir / "quality_report.html"
        quality_reporter.to_json(str(json_path))
        quality_reporter.to_html(str(html_path))

