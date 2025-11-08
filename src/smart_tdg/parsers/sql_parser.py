"""SQL DDL parser for extracting schema information."""

from typing import Dict, List, Optional
from simple_ddl_parser import DDLParser
from smart_tdg.models.schema_models import (
    Column, PrimaryKey, ForeignKey, CheckConstraint,
    UniqueConstraint, TableSchema, DatabaseSchema
)


class SQLSchemaParser:
    """Parser for SQL DDL statements."""
    
    def __init__(self, dialect: str = "postgresql"):
        self.dialect = dialect
    
    def parse(self, ddl_string: str) -> DatabaseSchema:
        """Parse SQL DDL and return DatabaseSchema."""
        # Parse using simple-ddl-parser
        parsed_tables = DDLParser(ddl_string).run()
        
        db_schema = DatabaseSchema(dialect=self.dialect)
        
        for table_def in parsed_tables:
            table_schema = self._parse_table(table_def)
            db_schema.add_table(table_schema)
        
        return db_schema
    
    def _parse_table(self, table_def: Dict) -> TableSchema:
        """Parse a single table definition."""
        table_name = table_def.get('table_name', '')
        
        # Parse columns
        columns = []
        for col_def in table_def.get('columns', []):
            column = self._parse_column(col_def)
            columns.append(column)
        
        # Parse primary key
        primary_key = self._parse_primary_key(table_def)
        
        # Parse foreign keys
        foreign_keys = self._parse_foreign_keys(table_def)
        
        # Parse check constraints
        check_constraints = self._parse_check_constraints(table_def, columns)
        
        # Parse unique constraints
        unique_constraints = self._parse_unique_constraints(table_def)
        
        return TableSchema(
            name=table_name,
            columns=columns,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
            check_constraints=check_constraints,
            unique_constraints=unique_constraints
        )
    
    def _parse_column(self, col_def: Dict) -> Column:
        """Parse a column definition."""
        name = col_def.get('name', '')
        col_type = col_def.get('type', 'VARCHAR')
        
        # Handle size in type (e.g., VARCHAR(100))
        if col_def.get('size'):
            col_type = f"{col_type}({col_def['size']})"
        
        # Nullable
        nullable = not col_def.get('nullable', True) is False
        
        # Default value
        default = col_def.get('default')
        
        # Check constraint
        check = col_def.get('check')
        
        # Unique
        unique = col_def.get('unique', False)
        
        return Column(
            name=name,
            type=col_type,
            nullable=nullable,
            default=default,
            check=check,
            unique=unique
        )
    
    def _parse_primary_key(self, table_def: Dict) -> Optional[PrimaryKey]:
        """Parse primary key constraint."""
        pk_cols = []
        
        # Check column-level primary keys
        for col_def in table_def.get('columns', []):
            if col_def.get('primary_key'):
                pk_cols.append(col_def['name'])
        
        # Check table-level primary keys
        if 'primary_key' in table_def and table_def['primary_key']:
            if isinstance(table_def['primary_key'], list):
                pk_cols.extend(table_def['primary_key'])
            else:
                pk_cols.append(table_def['primary_key'])
        
        if pk_cols:
            return PrimaryKey(columns=list(set(pk_cols)))
        
        return None
    
    def _parse_foreign_keys(self, table_def: Dict) -> List[ForeignKey]:
        """Parse foreign key constraints."""
        foreign_keys = []
        
        # Check column-level foreign keys
        for col_def in table_def.get('columns', []):
            if 'references' in col_def and col_def['references']:
                ref = col_def['references']
                fk = ForeignKey(
                    columns=[col_def['name']],
                    references_table=ref.get('table', ''),
                    references_columns=[ref.get('column', ref.get('columns', [''])[0])]
                )
                foreign_keys.append(fk)
        
        # Check table-level foreign keys
        for fk_def in table_def.get('alter', {}).get('foreign_keys', []):
            fk = ForeignKey(
                columns=fk_def.get('columns', []),
                references_table=fk_def.get('references', {}).get('table', ''),
                references_columns=fk_def.get('references', {}).get('columns', [])
            )
            foreign_keys.append(fk)
        
        return foreign_keys
    
    def _parse_check_constraints(self, table_def: Dict, columns: List[Column]) -> List[CheckConstraint]:
        """Parse CHECK constraints."""
        check_constraints = []
        
        # Column-level checks
        for col in columns:
            if col.check:
                check_constraints.append(
                    CheckConstraint(
                        expression=col.check,
                        columns=[col.name]
                    )
                )
        
        # Table-level checks
        for check_def in table_def.get('checks', []):
            if isinstance(check_def, dict):
                expr = check_def.get('constraint_name') or check_def.get('expression', '')
            else:
                expr = str(check_def)
            
            check_constraints.append(
                CheckConstraint(
                    expression=expr,
                    columns=[]  # TODO: Extract columns from expression
                )
            )
        
        return check_constraints
    
    def _parse_unique_constraints(self, table_def: Dict) -> List[UniqueConstraint]:
        """Parse UNIQUE constraints."""
        unique_constraints = []
        
        # Column-level unique
        for col_def in table_def.get('columns', []):
            if col_def.get('unique'):
                unique_constraints.append(
                    UniqueConstraint(columns=[col_def['name']])
                )
        
        # Table-level unique
        for unique_def in table_def.get('uniques', []):
            if isinstance(unique_def, dict):
                cols = unique_def.get('columns', [])
            else:
                cols = [str(unique_def)]
            
            if cols:
                unique_constraints.append(UniqueConstraint(columns=cols))
        
        return unique_constraints
