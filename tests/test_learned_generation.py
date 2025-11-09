import pandas as pd
from pathlib import Path
from src.smart_tdg.core.learned_data_generator import LearnedDataGenerator
from src.smart_tdg.core.schema_ingestion import SchemaIngestion
import pyarrow.parquet as pq

# Load parquet data files into dict
def load_parquet_folder(folder_path):
    data = {}
    folder = Path(folder_path)
    for parquet_file in folder.glob("*.parquet"):
        data[parquet_file.stem] = pd.read_parquet(parquet_file)
    return data

def test_learned_generation():
    parquet_data_dir = Path("examples/example1_retail_orders/inputs")
    data = load_parquet_folder(parquet_data_dir)
    
    # Load schema
    ingestion = SchemaIngestion()
    schema = ingestion.parse_parquet(str(next(parquet_data_dir.glob("*.parquet"))))  # or parse SQL
    
    generator = LearnedDataGenerator(data, ingestion.fk_graph)
    generator.train_models()
    synthetic_data = generator.generate_data({k: len(v) for k, v in data.items()})
    
    for table, df in synthetic_data.items():
        print(f"Generated {len(df)} rows for {table}")
        print(df.head())

if __name__ == "__main__":
    test_learned_generation()
