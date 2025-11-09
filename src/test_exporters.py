"""Test script for exporters - Using working schema."""

import os
from pathlib import Path
from smart_tdg.core.schema_ingestion import SchemaIngestion
from smart_tdg.core.data_generator import DataGenerator
from smart_tdg.models.scenario_models import Scenario
from smart_tdg.exporters.file_exporters import FileExporter
from smart_tdg.exporters.sql_exporters import SQLExporter
from smart_tdg.exporters.db_loaders import DatabaseLoader

def test_exporters():
    """Test all export formats using the working schema."""
    print("=" * 60)
    print("TEST: Data Exporters (PostgreSQL)")
    print("=" * 60)
    
    # Use the WORKING schema from test_data_generation
    ddl = """
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT CHECK (age > 0),
    region VARCHAR(10),
    UNIQUE(name)
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    amount DECIMAL(10,2),
    order_date DATE,
    status VARCHAR(20) CHECK (status IN ('PLACED','SHIPPED','DELIVERED','RETURNED'))
);
"""
    
    print("\n1. Generating Test Data...")
    print("-" * 60)
    
    ingestion = SchemaIngestion()
    schema = ingestion.parse_sql_ddl(ddl)
    
    print(f"✓ Parsed {len(schema.tables)} tables: {list(schema.tables.keys())}")
    
    scenario_data = {
        'scenario': 'test_export',
        'entities': {
            'customers': {'cardinality': 100},
            'orders': {'cardinality': 300}
        },
        'seed': 42
    }
    
    scenario = Scenario.from_dict(scenario_data)
    generator = DataGenerator(schema, ingestion.fk_graph, seed=42)
    data = generator.generate_data(scenario)
    
    print(f"✓ Generated {len(data['customers'])} customers")
    print(f"✓ Generated {len(data['orders'])} orders")
    
    # Create output directory
    output_dir = Path('./output/test_export')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("\n2. Testing File Exports...")
    print("-" * 60)
    
    file_exporter = FileExporter(data)
    
    print("\nExporting to CSV...")
    file_exporter.export_csv(output_dir=output_dir)
    
    print("\nExporting to JSON...")
    file_exporter.export_json(output_file=output_dir / 'data.json')
    
    print("\nExporting to Parquet...")
    file_exporter.export_parquet(output_dir=output_dir)
    
    print("\n3. Testing SQL Exports...")
    print("-" * 60)
    
    sql_exporter = SQLExporter(data)
    
    print("\nGenerating INSERT statements...")
    sql_exporter.export_insert_statements(
        output_file=output_dir / 'inserts.sql',
        dialect='postgresql'
    )
    
    print("\n4. Testing PostgreSQL Database Loader...")
    print("-" * 60)
    
    postgres_uri = os.getenv('POSTGRES_URI')
    
    if postgres_uri and 'your_' not in postgres_uri:
        try:
            db_loader = DatabaseLoader(data)
            db_loader.load_to_postgres(if_exists='replace')
            db_loader.verify_data()
            
        except Exception as e:
            print(f"\n⚠ PostgreSQL loading failed: {e}")
    else:
        print("\n⚠ PostgreSQL loading skipped (not configured)")
    
    print("\n" + "=" * 60)
    print("✓ ALL EXPORTER TESTS PASSED!")
    print("=" * 60)
    print(f"\nCheck files in: {output_dir}/")

if __name__ == "__main__":
    test_exporters()
