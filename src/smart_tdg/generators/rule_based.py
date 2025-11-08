"""Rule-based data generator using Faker library."""

from typing import Dict, List, Any, Union
import pandas as pd
import numpy as np
from faker import Faker
from smart_tdg.models.schema_models import DatabaseSchema, TableSchema, Column


class RuleBasedGenerator:
    """Generate data using Faker and rule-based logic."""
    
    def __init__(self, seed: int = 42):
        """Initialize rule-based generator."""
        self.seed = seed
        self.fake = Faker()
        Faker.seed(seed)
        np.random.seed(seed)
        
        # Semantic mappings for column names to Faker providers
        self.semantic_mappings = {
            'name': lambda: self.fake.name(),
            'first_name': lambda: self.fake.first_name(),
            'last_name': lambda: self.fake.last_name(),
            'email': lambda: self.fake.email(),
            'phone': lambda: self.fake.phone_number(),
            'address': lambda: self.fake.address(),
            'street': lambda: self.fake.street_address(),
            'city': lambda: self.fake.city(),
            'state': lambda: self.fake.state(),
            'country': lambda: self.fake.country(),
            'zipcode': lambda: self.fake.zipcode(),
            'zip': lambda: self.fake.zipcode(),
            'company': lambda: self.fake.company(),
            'job': lambda: self.fake.job(),
            'description': lambda: self.fake.text(max_nb_chars=200),
            'url': lambda: self.fake.url(),
            'username': lambda: self.fake.user_name(),
            'password': lambda: self.fake.password(),
            'date': lambda: self.fake.date_between(start_date='-5y', end_date='today'),
            'datetime': lambda: self.fake.date_time_between(start_date='-5y', end_date='now'),
            'time': lambda: self.fake.time(),
        }
    
    def generate_column_data(
        self, 
        column: Column, 
        row_count: int,
        distribution_config: Dict = None,
        enum_values: List = None
    ) -> List[Any]:
        """
        Generate data for a single column.
        
        Args:
            column: Column schema
            row_count: Number of rows to generate
            distribution_config: Optional distribution configuration
            enum_values: Optional enum values for categorical columns
        
        Returns:
            List of generated values
        """
        col_name_lower = column.name.lower()
        col_type_lower = column.type.lower()
        
        # Handle custom distributions
        if distribution_config:
            return self._generate_from_distribution(
                distribution_config, row_count, column.type
            )
        
        # Handle enum values
        if enum_values:
            return list(np.random.choice(enum_values, size=row_count))
        
        # Check semantic mappings first
        for keyword, generator in self.semantic_mappings.items():
            if keyword in col_name_lower:
                return [generator() for _ in range(row_count)]
        
        # Type-based generation
        if 'int' in col_type_lower or 'integer' in col_type_lower:
            return self._generate_integer(column, row_count)
        
        elif 'decimal' in col_type_lower or 'numeric' in col_type_lower or 'float' in col_type_lower:
            return self._generate_decimal(column, row_count)
        
        elif 'varchar' in col_type_lower or 'char' in col_type_lower or 'text' in col_type_lower or 'string' in col_type_lower:
            return self._generate_string(column, row_count)
        
        elif 'date' in col_type_lower and 'time' not in col_type_lower:
            return [self.fake.date_between(start_date='-5y', end_date='today') 
                    for _ in range(row_count)]
        
        elif 'timestamp' in col_type_lower or 'datetime' in col_type_lower:
            return [self.fake.date_time_between(start_date='-5y', end_date='now') 
                    for _ in range(row_count)]
        
        elif 'bool' in col_type_lower:
            return list(np.random.choice([True, False], size=row_count))
        
        else:
            # Default: string
            return [self.fake.word() for _ in range(row_count)]
    
    def _generate_integer(self, column: Column, row_count: int) -> List[int]:
        """Generate integer values."""
        # Check for CHECK constraints to determine range
        check_str = self._get_check_string(column.check)
        if check_str and '>' in check_str:
            min_val = 1
            max_val = 1000000
        else:
            min_val = 1
            max_val = 1000000
        
        return list(np.random.randint(min_val, max_val, size=row_count))
    
    def _generate_decimal(self, column: Column, row_count: int) -> List[float]:
        """Generate decimal values."""
        return list(np.random.uniform(0.01, 10000.0, size=row_count).round(2))
    
    def _generate_string(self, column: Column, row_count: int) -> List[str]:
        """Generate string values."""
        # Extract length from type if available (e.g., VARCHAR(100))
        max_length = 50
        if '(' in column.type and ')' in column.type:
            try:
                length_str = column.type.split('(')[1].split(')')[0]
                max_length = min(int(length_str), 200)
            except:
                pass
        
        return [self.fake.text(max_nb_chars=max_length)[:max_length] 
                for _ in range(row_count)]
    
    def _generate_from_distribution(
        self, 
        config: Dict, 
        row_count: int,
        col_type: str
    ) -> List[Any]:
        """Generate data from custom distribution config."""
        dist_type = config.get('type', 'uniform')
        
        if dist_type == 'categorical' or isinstance(config, dict) and all(isinstance(v, (int, float)) for v in config.values()):
            # Categorical distribution
            categories = list(config.keys())
            probabilities = list(config.values())
            # Normalize probabilities
            total = sum(probabilities)
            probabilities = [p / total for p in probabilities]
            
            return list(np.random.choice(categories, size=row_count, p=probabilities))
        
        elif dist_type == 'normal':
            mean = config.get('mean', 50)
            std = config.get('std', 10)
            values = np.random.normal(mean, std, size=row_count)
            
            if 'int' in col_type.lower():
                return list(values.astype(int))
            return list(values.round(2))
        
        elif dist_type == 'uniform':
            min_val = config.get('min', 0)
            max_val = config.get('max', 100)
            
            if 'int' in col_type.lower():
                return list(np.random.randint(min_val, max_val, size=row_count))
            return list(np.random.uniform(min_val, max_val, size=row_count).round(2))
        
        else:
            # Default uniform
            return list(np.random.uniform(0, 100, size=row_count).round(2))
    
    def generate_table_data(
        self,
        table_schema: TableSchema,
        row_count: int,
        scenario_config: Dict = None,
        parent_data: Dict[str, pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        Generate data for a complete table.
        
        Args:
            table_schema: Table schema definition
            row_count: Number of rows to generate
            scenario_config: Optional scenario configuration for this table
            parent_data: Data from parent tables (for FK resolution)
        
        Returns:
            DataFrame with generated data
        """
        data = {}
        
        for column in table_schema.columns:
            # Check if this is a foreign key column
            fk_info = self._get_fk_for_column(table_schema, column.name)
            
            if fk_info and parent_data:
                # Generate FK values from parent table
                parent_table = fk_info.references_table
                parent_column = fk_info.references_columns[0]
                
                if parent_table in parent_data:
                    parent_values = parent_data[parent_table][parent_column].values
                    data[column.name] = list(np.random.choice(parent_values, size=row_count))
                else:
                    # Fallback if parent data not available
                    data[column.name] = self.generate_column_data(column, row_count)
            else:
                # Get distribution config from scenario if available
                dist_config = None
                enum_values = None
                
                if scenario_config and 'distributions' in scenario_config:
                    dist_config = scenario_config['distributions'].get(column.name)
                
                # Check for enum values in CHECK constraints
                check_str = self._get_check_string(column.check)
                if check_str and 'IN' in check_str.upper():
                    enum_values = self._extract_enum_values(check_str)
                
                data[column.name] = self.generate_column_data(
                    column, row_count, dist_config, enum_values
                )
        
        df = pd.DataFrame(data)
        
        # Handle unique constraints
        for unique_col in table_schema.get_unique_columns():
            if unique_col in df.columns:
                # Ensure uniqueness by adding suffixes if needed
                df[unique_col] = self._ensure_uniqueness(df[unique_col].values)
        
        # Handle primary key uniqueness
        if table_schema.primary_key:
            for pk_col in table_schema.primary_key.columns:
                if pk_col in df.columns:
                    # For PK, use sequential IDs
                    df[pk_col] = range(1, len(df) + 1)
        
        return df
    
    def _get_check_string(self, check_constraint: Union[str, List, None]) -> str:
        """
        Convert check constraint to string, handling different types.
        
        Args:
            check_constraint: Can be str, list, or None
        
        Returns:
            String representation of check constraint
        """
        if check_constraint is None:
            return ""
        
        if isinstance(check_constraint, str):
            return check_constraint
        
        if isinstance(check_constraint, list):
            # Join list elements with space
            return " ".join(str(item) for item in check_constraint)
        
        return str(check_constraint)
    
    def _get_fk_for_column(self, table_schema: TableSchema, column_name: str):
        """Get foreign key info for a column."""
        for fk in table_schema.foreign_keys:
            if column_name in fk.columns:
                return fk
        return None
    
    def _extract_enum_values(self, check_constraint: str) -> List[str]:
        """Extract enum values from CHECK constraint."""
        # Parse formats like:
        # "status IN ('PLACED', 'SHIPPED', 'DELIVERED', 'RETURNED')"
        if not check_constraint or not isinstance(check_constraint, str):
            return []

        try:
            # Convert to uppercase for case-insensitive matching
            check_upper = check_constraint.upper()
            
            if 'IN' not in check_upper:
                return []
            
            # Find the content between parentheses after IN
            # Example: "status IN ('A', 'B', 'C')" -> extract "'A', 'B', 'C'"
            in_index = check_upper.find('IN')
            after_in = check_constraint[in_index + 2:].strip()
            
            # Find parentheses
            if '(' not in after_in or ')' not in after_in:
                return []
            start_paren = after_in.index('(')
            end_paren = after_in.index(')')

            values_str = after_in[start_paren + 1:end_paren]
            
            # Split by comma and clean up each value
            values = []
            for v in values_str.split(','):
                # Remove quotes, whitespace
                cleaned = v.strip().strip("'\"")
                if cleaned:
                    values.append(cleaned)
            
            if values:
                print(f"  ✓ Extracted enum values: {values}")
                return values
            
            return []
            
        except Exception as e:
            print(f"  Warning: Could not parse enum values from: {check_constraint}")
            print(f"    Error: {e}")
            return []

    
    def _ensure_uniqueness(self, values: np.ndarray) -> List:
        """Ensure all values are unique by adding suffixes."""
        result = []
        seen = {}
        
        for val in values:
            if val not in seen:
                result.append(val)
                seen[val] = 1
            else:
                # Add suffix to make unique
                suffix = seen[val]
                if isinstance(val, str):
                    result.append(f"{val}_{suffix}")
                else:
                    result.append(val + suffix)
                seen[val] += 1
        
        return result
