"""Main data generation orchestrator."""

from typing import Dict, Optional
import pandas as pd
from models.schema_models import DatabaseSchema
from models.scenario_models import Scenario
from generators.rule_based import RuleBasedGenerator
from utils.graph_utils import DependencyGraph


class DataGenerator:
    """Main data generation engine."""
    
    def __init__(self, schema: DatabaseSchema, fk_graph: DependencyGraph, seed: int = 42):
        """
        Initialize data generator.
        
        Args:
            schema: Database schema
            fk_graph: Foreign key dependency graph
            seed: Random seed for reproducibility
        """
        self.schema = schema
        self.fk_graph = fk_graph
        self.seed = seed
        self.rule_generator = RuleBasedGenerator(seed=seed)
        self.generated_data: Dict[str, pd.DataFrame] = {}
    
    def generate_data(
        self,
        scenario: Scenario,
        use_ml: bool = False,
        preview_only: bool = False
    ) -> Dict[str, pd.DataFrame]:
        """
        Generate data based on scenario.
        
        Args:
            scenario: Scenario configuration
            use_ml: Whether to use ML-based generation (future enhancement)
            preview_only: If True, generate only 10-50 rows for preview
        
        Returns:
            Dictionary mapping table names to DataFrames
        """
        # Get generation order (parent tables first)
        generation_order = self.fk_graph.topological_sort()
        
        self.generated_data = {}
        
        for table_name in generation_order:
            table_schema = self.schema.get_table(table_name)
            if not table_schema:
                continue
            
            # Get scenario config for this table
            table_scenario = scenario.tables.get(table_name)
            if not table_scenario:
                # Default scenario
                row_count = 10 if preview_only else 1000
                scenario_config = None
            else:
                row_count = 10 if preview_only else table_scenario.cardinality
                scenario_config = {
                    'distributions': table_scenario.distributions,
                    'constraints': table_scenario.constraints
                }
            
            print(f"\nGenerating {row_count} rows for table: {table_name}")
            
            # Generate data
            df = self.rule_generator.generate_table_data(
                table_schema=table_schema,
                row_count=row_count,
                scenario_config=scenario_config,
                parent_data=self.generated_data
            )
            
            self.generated_data[table_name] = df
            print(f"✓ Generated {len(df)} rows with {len(df.columns)} columns")
        
        return self.generated_data
    
    def get_data(self, table_name: str) -> Optional[pd.DataFrame]:
        """Get generated data for a specific table."""
        return self.generated_data.get(table_name)
    
    def get_preview(self, table_name: str, n: int = 10) -> Optional[pd.DataFrame]:
        """Get preview of generated data."""
        df = self.generated_data.get(table_name)
        if df is not None:
            return df.head(n)
        return None
