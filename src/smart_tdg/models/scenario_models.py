"""Data models for scenario definitions."""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from enum import Enum


class DistributionType(Enum):
    """Types of statistical distributions."""
    UNIFORM = "uniform"
    NORMAL = "normal"
    LOGNORMAL = "lognormal"
    EXPONENTIAL = "exponential"
    GAMMA = "gamma"
    BETA = "beta"
    CATEGORICAL = "categorical"


@dataclass
class DistributionConfig:
    """Configuration for a statistical distribution."""
    type: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CorrelationConfig:
    """Configuration for column correlation."""
    with_table: str
    with_column: str
    correlation: float = 0.5


@dataclass
class TemporalPattern:
    """Configuration for temporal patterns."""
    pattern_type: str  # e.g., "surge", "seasonal", "trend"
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TableScenario:
    """Scenario configuration for a single table."""
    cardinality: int
    distributions: Dict[str, Any] = field(default_factory=dict)
    correlations: List[CorrelationConfig] = field(default_factory=list)
    temporal_patterns: Dict[str, TemporalPattern] = field(default_factory=dict)
    constraints: Dict[str, Any] = field(default_factory=dict)
    invariants: List[str] = field(default_factory=list)
    edge_cases: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class Scenario:
    """Complete scenario definition."""
    name: str
    description: str = ""
    tables: Dict[str, TableScenario] = field(default_factory=dict)
    seed: int = 42
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Scenario':
        """Create Scenario from dictionary."""
        name = data.get('scenario', 'default_scenario')
        tables_data = data.get('entities', {})
        seed = data.get('seed', 42)
        
        tables = {}
        for table_name, table_data in tables_data.items():
            tables[table_name] = TableScenario(
                cardinality=table_data.get('cardinality', 1000),
                distributions=table_data.get('distribution', {}),
                correlations=[],
                temporal_patterns={},
                constraints=table_data.get('constraints', {}),
                invariants=[]
            )
            
            # Parse correlations
            if 'correlation' in table_data:
                corr_data = table_data['correlation']
                if isinstance(corr_data, dict):
                    tables[table_name].correlations.append(
                        CorrelationConfig(
                            with_table=corr_data.get('with', ''),
                            with_column=corr_data.get('key', ''),
                            correlation=0.8
                        )
                    )
            
            # Parse temporal patterns
            if 'temporal_pattern' in table_data:
                temp_data = table_data['temporal_pattern']
                if isinstance(temp_data, dict):
                    for pattern_name, pattern_value in temp_data.items():
                        tables[table_name].temporal_patterns[pattern_name] = TemporalPattern(
                            pattern_type=pattern_name,
                            params={'values': pattern_value}
                        )
        
        return cls(name=name, tables=tables, seed=seed)
