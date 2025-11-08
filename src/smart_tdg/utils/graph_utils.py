"""Graph utilities for foreign key dependency resolution."""

from typing import Dict, List, Set
from collections import defaultdict, deque


class DependencyGraph:
    """Directed graph for managing table dependencies via foreign keys."""
    
    def __init__(self):
        self.graph: Dict[str, List[str]] = defaultdict(list)
        self.reverse_graph: Dict[str, List[str]] = defaultdict(list)
    
    def add_edge(self, from_table: str, to_table: str):
        """Add a dependency edge (from_table depends on to_table)."""
        if to_table not in self.graph[from_table]:
            self.graph[from_table].append(to_table)
        if from_table not in self.reverse_graph[to_table]:
            self.reverse_graph[to_table].append(from_table)
    
    def topological_sort(self) -> List[str]:
        """
        Return tables in topological order (dependencies first).
        Uses Kahn's algorithm.
        """
        # Calculate in-degrees
        in_degree: Dict[str, int] = defaultdict(int)
        all_tables: Set[str] = set()
        
        # Collect all tables
        for table in self.graph.keys():
            all_tables.add(table)
            for dep in self.graph[table]:
                all_tables.add(dep)
        
        # Initialize in-degrees
        for table in all_tables:
            in_degree[table] = 0
        
        # Calculate in-degrees
        for table in self.graph:
            for dep in self.graph[table]:
                in_degree[table] += 1
        
        # Start with tables that have no dependencies
        queue = deque([table for table in all_tables if in_degree[table] == 0])
        result = []
        
        while queue:
            table = queue.popleft()
            result.append(table)
            
            # For each table that depends on current table
            for dependent in self.reverse_graph[table]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        # Check for cycles
        if len(result) != len(all_tables):
            raise ValueError("Circular dependency detected in foreign keys")
        
        return result
    
    def get_dependencies(self, table: str) -> List[str]:
        """Get list of tables that the given table depends on."""
        return self.graph.get(table, [])
    
    def get_dependents(self, table: str) -> List[str]:
        """Get list of tables that depend on the given table."""
        return self.reverse_graph.get(table, [])
