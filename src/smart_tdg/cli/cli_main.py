"""SmartTDG CLI - Scenario Engine integration."""

import click
import yaml
from core.learned_data_generator import LearnedDataGenerator
from core.scenario_engine import ScenarioEngine
from models.scenario_models import Scenario
from core.schema_ingestion import SchemaIngestion
from core.data_generator import DataGenerator
from exporters.file_exporters import FileExporter
from reporter.quality_reporter import QualityReporter
from pathlib import Path
import pandas as pd

@click.group()
def cli():
    """SmartTDG CLI Tool"""
    pass

@cli.command()
@click.option('--nl', prompt='Enter natural language scenario description',
              help='Natural language description of data scenario')
@click.option('--output', default='./scenarios/generated_scenario.yaml',
              help='Path to save generated YAML scenario')
def gen_scenario(nl: str, output: str):
    """Generate YAML Scenario from natural language using OpenAI."""
    try:
        engine = ScenarioEngine()
        scenario_dict = engine.generate_scenario(nl)
        
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            yaml.dump({'scenario': scenario_dict}, f, sort_keys=False)
        
        click.echo(f"✓ Scenario generated and saved to: {output_path}")
    except Exception as e:
        click.echo(f"✗ Error generating scenario: {e}")


@cli.command()
@click.option('--schema', required=True, type=click.Path(exists=True),
              help='Path to the input schema SQL file')
@click.option('--scenario', required=True, type=click.Path(exists=True),
              help='Path to the scenario YAML file')
@click.option('--seed', default=42, help='Random seed for generation')
@click.option('--output_dir', default='./output/generated_data', type=click.Path(),
              help='Output directory for generated data files')
@click.option('--generation_model', default='rule_based', type=click.Choice(['rule_based', 'learned']),
              help="Choose data generation model")
@click.option('--output_format', default='csv', type=click.Choice(['csv', 'parquet']),
              help='Format to export generated data')
def gen_data(schema, scenario, seed, output_dir, output_format, generation_model):
    """Generate data from schema and scenario YAML."""
    try:
        click.echo(f"Loading schema from: {schema}")
        with open(schema) as f:
            ddl = f.read()
        ingestion = SchemaIngestion()
        db_schema = ingestion.parse_sql_ddl(ddl)

        click.echo(f"Loading scenario from: {scenario}")
        with open(scenario) as f:
            scenario_dict = yaml.safe_load(f)
            # print(scenario_dict)
        scenario_obj = Scenario.from_dict(scenario_dict)
        print(scenario_obj.tables)

        if generation_model == 'learned':
            real_data = {}
            # Infer input data folder location based on schema file location 
            data_folder = Path(schema).parent / "inputs"
            for ext in ['*.parquet', '*.csv']:
                for file_path in data_folder.glob(ext):
                    if file_path.suffix.lower() == '.parquet':
                        real_data[file_path.stem] = pd.read_parquet(file_path)
                    elif file_path.suffix.lower() == '.csv':
                        real_data[file_path.stem] = pd.read_csv(file_path)
            generator = LearnedDataGenerator(real_data, ingestion.fk_graph)
            generator.train_models()
        else:
            generator = DataGenerator(db_schema, ingestion.fk_graph, seed=seed)
        if hasattr(scenario_obj, 'cardinalities'):
            # Case when 'cardinalities' is a dictionary
            data = generator.generate_data(scenario_obj.cardinalities)
        else:
            table_cardinalities = {table_name: table_scenario.cardinality for table_name, table_scenario in scenario_obj.tables.items()}
            data = generator.generate_data(table_cardinalities)

        # Export to CSV by default
        file_exporter = FileExporter(data)
        if output_format == 'csv':
            file_exporter.export_csv(output_dir)
        elif output_format == 'parquet':
            file_exporter.export_parquet(output_dir)
        click.echo(f"✓ Data exported as CSV to: {output_dir}")

    except Exception as e:
        click.echo(f"✗ Error generating data: {e}")


@cli.command()
@click.option('--data_dir', required=True, type=click.Path(exists=True),
              help='Input directory containing CSV data files (one per table)')
@click.option('--schema', required=True, type=click.Path(exists=True),
              help='Path to the input schema SQL file')
@click.option('--scenario', required=True, type=click.Path(exists=True),
              help='Path to the scenario YAML file')
@click.option('--output_dir', default='./output/quality_report', type=click.Path(),
              help='Output directory for quality report files')
def quality_report(data_dir, schema, scenario, output_dir):
    """Run data quality report on existing data and scenario."""
    try:
        click.echo("Loading data files...")
        import pandas as pd
        data = {}
        for file_path in Path(data_dir).glob('*'):
            if file_path.suffix.lower() in ['.csv', '.parquet']:
                table_name = file_path.stem
                if file_path.suffix.lower() == '.csv':
                    df = pd.read_csv(file_path)
                else:
                    df = pd.read_parquet(file_path)
                data[table_name] = df
        click.echo(f"Loaded tables: {list(data.keys())}")

        click.echo(f"Loading schema from: {schema}")
        with open(schema) as f:
            ddl = f.read()
        ingestion = SchemaIngestion()
        db_schema = ingestion.parse_sql_ddl(ddl)

        click.echo(f"Loading scenario from: {scenario}")
        with open(scenario) as f:
            scenario_dict = yaml.safe_load(f)
        scenario_obj = Scenario.from_dict(scenario_dict)

        click.echo("Running quality report...")
        reporter = QualityReporter(db_schema, scenario_obj, data)
        reporter.validate_all()
        reporter.print_summary()

        # Export quality report
        file_exporter = FileExporter(data)
        file_exporter.export_quality_report(reporter, output_dir=output_dir)
        click.echo(f"✓ Quality report exported to: {output_dir}")

    except Exception as e:
        click.echo(f"✗ Error running quality report: {e}")

if __name__ == "__main__":
    cli()

#C:/Users/akhileshn/Downloads/smart-test-data-generator/src/examples/example1_retail_orders/inputs/schema.sql
#C:/Users/akhileshn/Downloads/smart-test-data-generator/src/examples/example1_retail_orders/inputs/scenario.yaml
#C:/Users/akhileshn/Downloads/smart-test-data-generator/src/examples/example1_retail_orders/outputs