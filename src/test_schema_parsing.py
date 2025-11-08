"""Test script to validate schema ingestion."""

import json
from pathlib import Path
from smart_tdg.core.schema_ingestion import SchemaIngestion
from smart_tdg.models.scenario_models import Scenario

def test_sql_parsing():
    """Test SQL DDL parsing with the provided schema.sql"""
    print("=" * 60)
    print("TEST 1: SQL DDL Parsing")
    print("=" * 60)
    
    # Your schema.sql content
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
    
    print(f"\n✓ Parsed {len(schema.tables)} tables")
    
    # Test customers table
    customers = schema.get_table('customers')
    print(f"\n✓ Table: {customers.name}")
    print(f"  - Columns: {[col.name for col in customers.columns]}")
    print(f"  - Primary Key: {customers.primary_key.columns if customers.primary_key else None}")
    print(f"  - Unique Constraints: {len(customers.unique_constraints)}")
    print(f"  - Check Constraints: {len(customers.check_constraints)}")
    
    # Test orders table
    orders = schema.get_table('orders')
    print(f"\n✓ Table: {orders.name}")
    print(f"  - Columns: {[col.name for col in orders.columns]}")
    print(f"  - Primary Key: {orders.primary_key.columns if orders.primary_key else None}")
    print(f"  - Foreign Keys: {len(orders.foreign_keys)}")
    if orders.foreign_keys:
        fk = orders.foreign_keys[0]
        print(f"    → {fk.columns} references {fk.references_table}({fk.references_columns})")
    
    # Test generation order
    gen_order = ingestion.get_generation_order()
    print(f"\n✓ Generation Order: {gen_order}")
    print("  (Parent tables first, then child tables)")
    
    # Test schema summary
    summary = ingestion.get_schema_summary()
    print(f"\n✓ Schema Summary:")
    print(json.dumps(summary, indent=2))
    
    return schema, ingestion


def test_json_schema_parsing():
    """Test JSON schema parsing with the provided schema_parquet.json"""
    print("\n" + "=" * 60)
    print("TEST 2: JSON/Parquet Schema Parsing")
    print("=" * 60)
    
    # Your schema_parquet.json content
    json_schema = {
        "schema": {
            "fields": [
                {"name": "customer_id", "type": "int32"},
                {"name": "name", "type": "string"},
                {"name": "region", "type": "string"},
                {"name": "age", "type": "int32"}
            ]
        }
    }
    
    ingestion = SchemaIngestion()
    
    # Save to temp file for testing
    temp_file = Path("temp_schema.json")
    with open(temp_file, 'w') as f:
        json.dump(json_schema, f)
    
    schema = ingestion.parse_json_schema(str(temp_file))
    
    print(f"\n✓ Parsed {len(schema.tables)} tables")
    
    for table_name, table in schema.tables.items():
        print(f"\n✓ Table: {table_name}")
        print(f"  - Columns: {[(col.name, col.type) for col in table.columns]}")
    
    # Cleanup
    temp_file.unlink()
    
    return schema


def test_scenario_parsing():
    """Test scenario YAML parsing"""
    print("\n" + "=" * 60)
    print("TEST 3: Scenario YAML Parsing")
    print("=" * 60)
    
    # Your scenario.yaml content as dict
    scenario_data = {
        'scenario': 'month_end_surge',
        'entities': {
            'customers': {
                'cardinality': 10000,
                'distribution': {
                    'region': {"tier1": 0.6, "tier2": 0.4}
                }
            },
            'orders': {
                'cardinality': 50000,
                'correlation': {
                    'with': 'customers',
                    'key': 'customer_id'
                },
                'constraints': {
                    'fraud_rate': 0.03,
                    'return_rate_tier2': 0.08
                },
                'temporal_pattern': {
                    'daily_volume': ['low', 'medium', 'high']
                }
            }
        },
        'seed': 42
    }
    
    scenario = Scenario.from_dict(scenario_data)
    
    print(f"\n✓ Scenario: {scenario.name}")
    print(f"✓ Seed: {scenario.seed}")
    print(f"✓ Tables: {list(scenario.tables.keys())}")
    
    for table_name, table_scenario in scenario.tables.items():
        print(f"\n✓ Table: {table_name}")
        print(f"  - Cardinality: {table_scenario.cardinality}")
        print(f"  - Distributions: {table_scenario.distributions}")
        print(f"  - Constraints: {table_scenario.constraints}")
        print(f"  - Correlations: {len(table_scenario.correlations)}")
    
    return scenario


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("SMART TEST DATA GENERATOR - VALIDATION TESTS")
    print("=" * 60)
    
    try:
        # Test 1: SQL Parsing
        sql_schema, ingestion = test_sql_parsing()
        
        # Test 2: JSON Schema Parsing
        json_schema = test_json_schema_parsing()
        
        # Test 3: Scenario Parsing
        scenario = test_scenario_parsing()
        
        print("\n" + "=" * 60)
        print("✓ ALL TESTS PASSED!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ TEST FAILED: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
