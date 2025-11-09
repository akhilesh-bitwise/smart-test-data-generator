from smart_tdg.core.schema_ingestion import SchemaIngestion
from smart_tdg.core.data_generator import DataGenerator
from smart_tdg.models.scenario_models import Scenario
from smart_tdg.reporter.quality_reporter import QualityReporter
from smart_tdg.exporters.file_exporters import FileExporter

def test_quality():
    ddl = '''
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
'''
    ingestion = SchemaIngestion()
    schema = ingestion.parse_sql_ddl(ddl)
    scenario_data = {
        'scenario': 'month_end_surge',
        'entities': {
            'customers': {'cardinality': 100, 'distribution': {'region': {'tier1': 0.6, 'tier2': 0.4}}},
            'orders': {'cardinality': 300, 'distribution': {'status': {'PLACED': 0.25, 'SHIPPED':0.25, 'DELIVERED':0.25, 'RETURNED':0.25}}}
        },
        'seed': 42
    }
    scenario = Scenario.from_dict(scenario_data)
    generator = DataGenerator(schema, ingestion.fk_graph, seed=42)
    data = generator.generate_data(scenario)
    reporter = QualityReporter(schema, scenario, data)
    reporter.validate_all()
    reporter.print_summary()
    # Export as JSON and HTML as well
    file_exporter = FileExporter(data)
    file_exporter.export_quality_report(reporter, output_dir="./output/quality")

if __name__ == "__main__":
    test_quality()
