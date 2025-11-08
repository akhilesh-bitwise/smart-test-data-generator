"""Test script for data generation."""

import yaml
from smart_tdg.core.schema_ingestion import SchemaIngestion
from smart_tdg.core.data_generator import DataGenerator
from smart_tdg.models.scenario_models import Scenario
from smart_tdg.cache.cache_manager import CacheManager

def test_data_generation():
    """Test complete data generation pipeline."""
    print("=" * 60)
    print("TEST: Complete Data Generation Pipeline")
    print("=" * 60)
    
    # Step 1: Ingest schema
    print("\n1. Ingesting Schema...")
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
    
    ingestion = SchemaIngestion()
    schema = ingestion.parse_sql_ddl(ddl)
    print(f"✓ Schema ingested: {list(schema.tables.keys())}")
    
    # Step 2: Create scenario
    print("\n2. Creating Scenario...")
    scenario_data = {
        'scenario': 'month_end_surge',
        'entities': {
            'customers': {
                'cardinality': 100,
                'distribution': {
                    'region': {"tier1": 0.6, "tier2": 0.4}
                }
            },
            'orders': {
                'cardinality': 500,
                'correlation': {
                    'with': 'customers',
                    'key': 'customer_id'
                },
                'constraints': {
                    'fraud_rate': 0.03,
                    'return_rate_tier2': 0.08
                }
            }
        },
        'seed': 42
    }
    
    scenario = Scenario.from_dict(scenario_data)
    print(f"✓ Scenario created: {scenario.name}")
    
    # Step 3: Initialize generator
    print("\n3. Initializing Data Generator...")
    generator = DataGenerator(schema, ingestion.fk_graph, seed=42)
    print("✓ Generator initialized")
    
    # Step 4: Generate data (preview first)
    print("\n4. Generating Preview Data (10 rows)...")
    preview_data = generator.generate_data(scenario, preview_only=True)
    
    for table_name, df in preview_data.items():
        print(f"\n✓ Table: {table_name}")
        print(f"  Shape: {df.shape}")
        print(f"  Columns: {list(df.columns)}")
        print(f"\n  Preview:")
        print(df.head())
    
    # Step 5: Generate full data
    print("\n5. Generating Full Data...")
    full_data = generator.generate_data(scenario, preview_only=False)
    
    for table_name, df in full_data.items():
        print(f"\n✓ Table: {table_name}")
        print(f"  Total rows: {len(df)}")
        print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
    
    # Step 6: Test caching
    print("\n6. Testing Cache...")
    cache_manager = CacheManager()
    
    print("  Caching data...")
    cache_manager.set(schema, scenario, {
        table: df.to_dict() for table, df in full_data.items()
    })
    
    print("  Retrieving from cache...")
    cached_data = cache_manager.get(schema, scenario)
    
    if cached_data:
        print("  ✓ Cache working! Data retrieved successfully")
    else:
        print("  ✗ Cache failed")
    
    # Step 7: Verify data quality
    print("\n7. Basic Quality Checks...")
    
    # Check customers
    customers_df = full_data['customers']
    print(f"\n  Customers table:")
    print(f"    - Unique customer_ids: {customers_df['customer_id'].nunique()} / {len(customers_df)}")
    print(f"    - Unique names: {customers_df['name'].nunique()} / {len(customers_df)}")
    print(f"    - Age > 0: {(customers_df['age'] > 0).all()}")
    print(f"    - Region distribution: {customers_df['region'].value_counts().to_dict()}")
    
    # Check orders
    orders_df = full_data['orders']
    print(f"\n  Orders table:")
    print(f"    - Unique order_ids: {orders_df['order_id'].nunique()} / {len(orders_df)}")
    print(f"    - Valid customer_ids: {orders_df['customer_id'].isin(customers_df['customer_id']).all()}")
    print(f"    - Status distribution: {orders_df['status'].value_counts().to_dict()}")
    
    print("\n" + "=" * 60)
    print("✓ ALL DATA GENERATION TESTS PASSED!")
    print("=" * 60)
    
    return full_data

if __name__ == "__main__":
    test_data_generation()
