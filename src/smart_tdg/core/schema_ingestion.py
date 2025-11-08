"""Schema ingestion and inference engine."""

from typing import Dict, Optional, List
from pathlib import Path
from smart_tdg.models.schema_models import DatabaseSchema, ForeignKey
from smart_tdg.parsers.sql_parser import SQLSchemaParser
from smart_tdg.parsers.openapi_parser import OpenAPISchemaParser
from smart_tdg.utils.graph_utils import DependencyGraph


class SchemaIngestion:
    """Main schema ingestion engine."""
    
    def __init__(self):
        self.schema: Optional[DatabaseSchema] = None
        self.fk_graph: Optional[DependencyGraph] = None
        self.sql_parser = SQLSchemaParser()
        self.openapi_parser = OpenAPISchemaParser()
    
    def parse_sql_ddl(self, ddl_string: str, dialect: str = 'postgresql') -> DatabaseSchema:
        """
        Parse SQL DDL string and extract schema.
        
        Args:
            ddl_string: SQL DDL statements
            dialect: SQL dialect (postgresql, mysql, sqlserver)
        
        Returns:
            DatabaseSchema object
        """
        self.sql_parser.dialect = dialect
        self.schema = self.sql_parser.parse(ddl_string)
        self._build_fk_graph()
        return self.schema
    
    def parse_sql_file(self, file_path: str, dialect: str = 'postgresql') -> DatabaseSchema:
        """Parse SQL DDL from file."""
        with open(file_path, 'r') as f:
            ddl_string = f.read()
        return self.parse_sql_ddl(ddl_string, dialect)
    
    def parse_openapi(self, file_path: str) -> DatabaseSchema:
        """Parse OpenAPI specification file."""
        self.schema = self.openapi_parser.parse_file(file_path)
        self._build_fk_graph()
        return self.schema
    
    def parse_json_schema(self, file_path: str) -> DatabaseSchema:
        """Parse JSON Schema or Parquet schema file."""
        self.schema = self.openapi_parser.parse_file(file_path)
        self._build_fk_graph()
        return self.schema
    
    def parse_parquet(self, file_path: str) -> DatabaseSchema:
        """Parse Parquet file and extract schema."""
        import pyarrow.parquet as pq
        
        # Read Parquet file schema
        parquet_file = pq.ParquetFile(file_path)
        schema = parquet_file.schema_arrow
        
        # Convert to our schema format
        from smart_tdg.models.schema_models import TableSchema, Column
        
        table_name = Path(file_path).stem
        columns = []
        
        for field in schema:
            col = Column(
                name=field.name,
                type=str(field.type),
                nullable=field.nullable
            )
            columns.append(col)
        
        table_schema = TableSchema(name=table_name, columns=columns)
        
        self.schema = DatabaseSchema()
        self.schema.add_table(table_schema)
        self._build_fk_graph()
        
        return self.schema
    
    def _build_fk_graph(self):
        """Build foreign key dependency graph."""
        if not self.schema:
            return
        
        self.fk_graph = DependencyGraph()
        
        for table_name, table_schema in self.schema.tables.items():
            for fk in table_schema.foreign_keys:
                # Add edge: child table depends on parent table
                self.fk_graph.add_edge(table_name, fk.references_table)
    
    def get_generation_order(self) -> List[str]:
        """
        Get table generation order based on FK dependencies.
        Parent tables come before child tables.
        """
        if not self.fk_graph:
            return list(self.schema.tables.keys()) if self.schema else []
        
        return self.fk_graph.topological_sort()
    
    def get_table_dependencies(self, table_name: str) -> List[str]:
        """Get list of tables that the given table depends on."""
        if not self.fk_graph:
            return []
        return self.fk_graph.get_dependencies(table_name)
    
    def get_schema_summary(self) -> Dict:
        """Get a summary of the ingested schema."""
        if not self.schema:
            return {}
        
        summary = {
            'dialect': self.schema.dialect,
            'table_count': len(self.schema.tables),
            'tables': {}
        }
        
        for table_name, table_schema in self.schema.tables.items():
            summary['tables'][table_name] = {
                'column_count': len(table_schema.columns),
                'columns': [col.name for col in table_schema.columns],
                'primary_key': table_schema.primary_key.columns if table_schema.primary_key else None,
                'foreign_keys': len(table_schema.foreign_keys),
                'check_constraints': len(table_schema.check_constraints),
                'unique_constraints': len(table_schema.unique_constraints)
            }
        
        if self.fk_graph:
            summary['generation_order'] = self.get_generation_order()
        
        return summary
