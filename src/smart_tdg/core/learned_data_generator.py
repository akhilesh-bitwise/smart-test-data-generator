import pandas as pd
from typing import Dict, Optional, List
from ctgan import CTGAN
from core.fk_utils import enforce_foreign_keys  # Utility to enforce FK after generation


class LearnedDataGenerator:
    """
    Learned synthetic data generator using CTGAN or similar models.
    Trains per-table generative models and generates synthetic data preserving correlations.
    """

    def __init__(self, real_data: Dict[str, pd.DataFrame], fk_graph: Optional[object] = None):
        """
        Args:
            real_data: Dict of table_name -> DataFrame with real data for training
            fk_graph: Foreign key dependency graph for enforcing referential integrity
        """
        self.real_data = real_data
        self.fk_graph = fk_graph
        self.models: Dict[str, CTGAN] = {}

    def train_models(self):
        """
        Train CTGAN models for each table using the provided real data.
        """
        for table_name, df in self.real_data.items():
            # logger.info(f"Training CTGAN model for table '{table_name}' with {len(df)} rows")
            model = CTGAN()
            model.fit(df)
            self.models[table_name] = model
        # logger.info("All CTGAN models trained.")

    def generate_data(self, cardinalities: Dict[str, int]) -> Dict[str, pd.DataFrame]:
        """
        Generate synthetic data for each table based on trained models.

        Args:
            cardinalities: Dict mapping table_name to number of rows to generate

        Returns:
            Dict of table_name -> synthetic DataFrame
        """
        synthetic_data = {}
        for table_name, model in self.models.items():
            num_rows = cardinalities.get(table_name, 1000)
            # logger.info(f"Generating {num_rows} rows for table '{table_name}'")
            sampled_data = model.sample(num_rows)
            synthetic_data[table_name] = sampled_data

        # Enforce foreign key constraints across tables if fk_graph provided
        if self.fk_graph:
            synthetic_data = enforce_foreign_keys(synthetic_data, self.fk_graph)

        return synthetic_data

    def generate_edge_cases(self, synthetic_data: Dict[str, pd.DataFrame], config: Optional[dict] = None):
        """
        Inject edge cases like null bursts, outliers, duplicates, or constraint violations
        into synthetic data based on provided configuration.
        Placeholder for your edge case logic.
        """
        # Implement as needed, e.g.:
        # - Add nulls randomly in columns with null bursts
        # - Inject outlier numeric values per config
        # - Introduce duplicates on demand

        return synthetic_data
