"""SQL DDL parser - FIXED VERSION."""

from typing import Dict, List, Optional
from simple_ddl_parser import DDLParser
from smart_tdg.models.schema_models import (
    Column, PrimaryKey, ForeignKey, CheckConstraint,
    UniqueConstraint, TableSchema, DatabaseSchema
)

print("=" * 60)
print("PARSER FILE LOADED - NEW VERSION")
print("=" * 60)


class SQLSchemaParser:
    """Parser for SQL DDL statements."""
    
    def __init__(self, dialect: str = "postgresql"):
        self.dialect = dialect
    
    def parse(self, ddl_string: str) -> DatabaseSchema:
        """Parse SQL DDL and return DatabaseSchema."""
        parsed_tables = DDLParser(ddl_string).run()
        db_schema = DatabaseSchema(dialect=self.dialect)
        
        for table_def in parsed_tables:
            table_schema = self._parse_table(table_def)
            db_schema.add_table(table_schema)
        
        return db_schema
    
    def _parse_table(self, table_def: Dict) -> TableSchema:
        """Parse a single table definition."""
        table_name = table_def.get('table_name', '')
        columns = [self._parse_column(col_def) for col_def in table_def.get('columns', [])]
        
        return TableSchema(
            name=table_name,
            columns=columns,
            primary_key=self._parse_primary_key(table_def),
            foreign_keys=self._parse_foreign_keys(table_def),
            check_constraints=self._parse_check_constraints(table_def, columns),
            unique_constraints=self._parse_unique_constraints(table_def)
        )
    
    def _parse_column(self, col_def: Dict) -> Column:
        """Parse a column definition - WITH PROPER CHECK HANDLING."""
        name = col_def.get('name', '')
        col_type = col_def.get('type', 'VARCHAR')
        
        if col_def.get('size'):
            col_type = f"{col_type}({col_def['size']})"
        
        # THE KEY FIX: Extract and normalize check constraint
        check_raw = col_def.get('check')
        check_normalized = None
        
        if check_raw:
            print(f"    Raw CHECK for {name}: {type(check_raw)} = {check_raw}")  # ADD THIS

            # HANDLE LIST FORMAT - NEW!
            if isinstance(check_raw, list) and len(check_raw) > 0:
                # Extract first element if it's a list
                check_raw = check_raw[0]
                print(f"    Extracted from list: {type(check_raw)} = {check_raw}")

            if isinstance(check_raw, dict) and 'in_statement' in check_raw:
                # Handle dict format
                in_stmt = check_raw['in_statement']
                col_name = in_stmt.get('name', name)
                values = in_stmt.get('in', [])
                
                # Clean quotes from values
                clean_values = [str(v).strip().strip("'\"") for v in values]
                values_str = ', '.join(f"'{v}'" for v in clean_values)
                check_normalized = f"{col_name} IN ({values_str})"
                print(f"      ✓ Fixed CHECK: {check_normalized}")
                
            elif isinstance(check_raw, str):
                check_normalized = check_raw
            else:
                check_normalized = str(check_raw)
        
        return Column(
            name=name,
            type=col_type,
            nullable=not col_def.get('nullable', True) is False,
            default=col_def.get('default'),
            check=check_normalized,
            unique=col_def.get('unique', False)
        )
    
    def _parse_primary_key(self, table_def: Dict) -> Optional[PrimaryKey]:
        pk_cols = []
        for col_def in table_def.get('columns', []):
            if col_def.get('primary_key'):
                pk_cols.append(col_def['name'])
        
        if 'primary_key' in table_def and table_def['primary_key']:
            if isinstance(table_def['primary_key'], list):
                pk_cols.extend(table_def['primary_key'])
            else:
                pk_cols.append(table_def['primary_key'])
        
        return PrimaryKey(columns=list(set(pk_cols))) if pk_cols else None
    
    def _parse_foreign_keys(self, table_def: Dict) -> List[ForeignKey]:
        foreign_keys = []
        
        for col_def in table_def.get('columns', []):
            if 'references' in col_def and col_def['references']:
                ref = col_def['references']
                foreign_keys.append(ForeignKey(
                    columns=[col_def['name']],
                    references_table=ref.get('table', ''),
                    references_columns=[ref.get('column', ref.get('columns', [''])[0])]
                ))
        
        return foreign_keys
    
    def _parse_check_constraints(self, table_def: Dict, columns: List[Column]) -> List[CheckConstraint]:
        return [CheckConstraint(expression=col.check, columns=[col.name]) 
                for col in columns if col.check]
    
    def _parse_unique_constraints(self, table_def: Dict) -> List[UniqueConstraint]:
        unique_constraints = []
        
        for col_def in table_def.get('columns', []):
            if col_def.get('unique'):
                unique_constraints.append(UniqueConstraint(columns=[col_def['name']]))
        
        return unique_constraints
