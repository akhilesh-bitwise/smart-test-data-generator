from typing import Dict
import pandas as pd

def enforce_foreign_keys(synthetic_data: Dict[str, pd.DataFrame], fk_graph) -> Dict[str, pd.DataFrame]:
    """
    Ensure foreign key columns in child tables reference valid keys in parent tables.
    This can be done by replacing FK values with sampled valid parent keys.

    Args:
        synthetic_data: Dict mapping table_name -> DataFrame with generated synthetic data
        fk_graph: Foreign key dependency graph that provides ForeignKey constraints info

    Returns:
        Updated synthetic_data dict with FK integrity ensured
    """
    # Iterate over tables and their FKs using fk_graph
    for table_name, table_fks in fk_graph.items():
        child_df = synthetic_data.get(table_name)
        if child_df is None:
            continue
        
        for fk in table_fks:
            parent_table = fk.references_table
            parent_df = synthetic_data.get(parent_table)
            if parent_df is None:
                continue
            
            parent_keys = parent_df[fk.references_columns]
            # Sample FK column values from parent keys
            # If multiple columns, handle composite keys appropriately
            
            if isinstance(fk.columns, list):
                # Composite FK
                for child_col, parent_col in zip(fk.columns, fk.references_columns):
                    child_df[child_col] = parent_df[parent_col].sample(n=len(child_df), replace=True).reset_index(drop=True)
            else:
                # Single column
                child_df[fk.columns] = parent_df[fk.references_columns].sample(n=len(child_df), replace=True).reset_index(drop=True)
            
            # Update synthetic data dict
            synthetic_data[table_name] = child_df

    return synthetic_data
