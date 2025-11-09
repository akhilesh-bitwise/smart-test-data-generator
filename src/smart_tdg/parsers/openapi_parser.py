"""OpenAPI and JSON Schema parser for extracting schema information."""

import json
from typing import Dict, Any
from pathlib import Path
from models.schema_models import (
    Column, TableSchema, DatabaseSchema
)


class OpenAPISchemaParser:
    """Parser for OpenAPI specifications and JSON Schema."""
    
    def __init__(self):
        self.type_mapping = {
            'string': 'VARCHAR(255)',
            'integer': 'INT',
            'int32': 'INT',
            'int64': 'BIGINT',
            'number': 'DECIMAL',
            'boolean': 'BOOLEAN',
            'array': 'JSON',
            'object': 'JSON'
        }
    
    def parse_file(self, file_path: str) -> DatabaseSchema:
        """Parse OpenAPI or JSON Schema file."""
        with open(file_path, 'r') as f:
            schema_data = json.load(f)
        
        return self.parse_dict(schema_data)
    
    def parse_dict(self, schema_data: Dict[str, Any]) -> DatabaseSchema:
        """Parse schema from dictionary."""
        db_schema = DatabaseSchema(dialect='json_schema')
        
        # Check if it's Parquet schema format
        if 'schema' in schema_data and 'fields' in schema_data['schema']:
            # Parquet schema format
            table_schema = self._parse_parquet_schema(schema_data)
            db_schema.add_table(table_schema)
        
        # Check if it's OpenAPI format
        elif 'openapi' in schema_data or 'swagger' in schema_data:
            # OpenAPI format
            tables = self._parse_openapi_schema(schema_data)
            for table in tables:
                db_schema.add_table(table)
        
        # Generic JSON Schema
        elif 'properties' in schema_data:
            table_schema = self._parse_json_schema(schema_data, 'main_table')
            db_schema.add_table(table_schema)
        
        return db_schema
    
    def _parse_parquet_schema(self, schema_data: Dict[str, Any]) -> TableSchema:
        """Parse Parquet schema format."""
        fields = schema_data.get('schema', {}).get('fields', [])
        table_name = schema_data.get('name', 'parquet_table')
        
        columns = []
        for field in fields:
            col = Column(
                name=field.get('name', ''),
                type=self._map_type(field.get('type', 'string')),
                nullable=field.get('nullable', True)
            )
            columns.append(col)
        
        return TableSchema(name=table_name, columns=columns)
    
    def _parse_openapi_schema(self, schema_data: Dict[str, Any]) -> list:
        """Parse OpenAPI schema format."""
        tables = []
        
        # Extract schemas from components
        components = schema_data.get('components', {})
        schemas = components.get('schemas', {})
        
        for schema_name, schema_def in schemas.items():
            if schema_def.get('type') == 'object':
                table = self._parse_json_schema(schema_def, schema_name)
                tables.append(table)
        
        return tables
    
    def _parse_json_schema(self, schema_def: Dict[str, Any], table_name: str) -> TableSchema:
        """Parse JSON Schema object."""
        properties = schema_def.get('properties', {})
        required = schema_def.get('required', [])
        
        columns = []
        for prop_name, prop_def in properties.items():
            col_type = self._map_type(prop_def.get('type', 'string'))
            col = Column(
                name=prop_name,
                type=col_type,
                nullable=prop_name not in required,
                default=prop_def.get('default')
            )
            columns.append(col)
        
        return TableSchema(name=table_name, columns=columns)
    
    def _map_type(self, json_type: str) -> str:
        """Map JSON/Parquet types to SQL types."""
        return self.type_mapping.get(json_type.lower(), 'VARCHAR(255)')
