"""Data models for schema representation."""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from enum import Enum


class ColumnType(Enum):
    """Supported column data types."""
    INTEGER = "integer"
    INT = "int"
    BIGINT = "bigint"
    SMALLINT = "smallint"
    DECIMAL = "decimal"
    NUMERIC = "numeric"
    FLOAT = "float"
    DOUBLE = "double"
    VARCHAR = "varchar"
    CHAR = "char"
    TEXT = "text"
    STRING = "string"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    DATETIME = "datetime"
    TIME = "time"
    JSON = "json"
    ARRAY = "array"
    ENUM = "enum"


@dataclass
class Column:
    """Represents a database column."""
    name: str
    type: str
    nullable: bool = True
    default: Optional[Any] = None
    check: Optional[str] = None
    unique: bool = False
    comment: Optional[str] = None
    
    def __repr__(self):
        return f"Column(name={self.name}, type={self.type}, nullable={self.nullable})"


@dataclass
class PrimaryKey:
    """Represents a primary key constraint."""
    columns: List[str]
    name: Optional[str] = None
    
    def __repr__(self):
        return f"PrimaryKey({self.columns})"


@dataclass
class ForeignKey:
    """Represents a foreign key constraint."""
    columns: List[str]
    references_table: str
    references_columns: List[str]
    name: Optional[str] = None
    on_delete: Optional[str] = None
    on_update: Optional[str] = None
    
    def __repr__(self):
        return f"ForeignKey({self.columns} -> {self.references_table}.{self.references_columns})"


@dataclass
class CheckConstraint:
    """Represents a CHECK constraint."""
    expression: str
    columns: List[str]
    name: Optional[str] = None
    
    def __repr__(self):
        return f"CheckConstraint({self.expression})"


@dataclass
class UniqueConstraint:
    """Represents a UNIQUE constraint."""
    columns: List[str]
    name: Optional[str] = None
    
    def __repr__(self):
        return f"UniqueConstraint({self.columns})"


@dataclass
class TableSchema:
    """Represents a complete table schema."""
    name: str
    columns: List[Column] = field(default_factory=list)
    primary_key: Optional[PrimaryKey] = None
    foreign_keys: List[ForeignKey] = field(default_factory=list)
    check_constraints: List[CheckConstraint] = field(default_factory=list)
    unique_constraints: List[UniqueConstraint] = field(default_factory=list)
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    
    def get_column(self, column_name: str) -> Optional[Column]:
        """Get column by name."""
        for col in self.columns:
            if col.name == column_name:
                return col
        return None
    
    def get_not_null_columns(self) -> List[str]:
        """Get list of NOT NULL column names."""
        return [col.name for col in self.columns if not col.nullable]
    
    def get_unique_columns(self) -> List[str]:
        """Get list of UNIQUE column names."""
        unique_cols = [col.name for col in self.columns if col.unique]
        for uc in self.unique_constraints:
            unique_cols.extend(uc.columns)
        return list(set(unique_cols))
    
    def __repr__(self):
        return f"TableSchema(name={self.name}, columns={len(self.columns)})"


@dataclass
class DatabaseSchema:
    """Represents a complete database schema."""
    tables: Dict[str, TableSchema] = field(default_factory=dict)
    version: Optional[str] = None
    dialect: str = "postgresql"
    
    def get_table(self, table_name: str) -> Optional[TableSchema]:
        """Get table schema by name."""
        return self.tables.get(table_name)
    
    def add_table(self, table: TableSchema):
        """Add a table to the schema."""
        self.tables[table.name] = table
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'version': self.version,
            'dialect': self.dialect,
            'tables': {
                name: {
                    'columns': [
                        {
                            'name': col.name,
                            'type': col.type,
                            'nullable': col.nullable,
                            'default': col.default,
                            'unique': col.unique,
                            'check': col.check
                        }
                        for col in table.columns
                    ],
                    'primary_key': table.primary_key.columns if table.primary_key else [],
                    'foreign_keys': [
                        {
                            'columns': fk.columns,
                            'references_table': fk.references_table,
                            'references_columns': fk.references_columns
                        }
                        for fk in table.foreign_keys
                    ],
                    'check_constraints': [
                        {'expression': cc.expression, 'columns': cc.columns}
                        for cc in table.check_constraints
                    ],
                    'unique_constraints': [
                        {'columns': uc.columns}
                        for uc in table.unique_constraints
                    ]
                }
                for name, table in self.tables.items()
            }
        }
    
    def __repr__(self):
        return f"DatabaseSchema(tables={list(self.tables.keys())}, dialect={self.dialect})"
