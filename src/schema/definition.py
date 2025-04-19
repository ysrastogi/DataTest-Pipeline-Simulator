"""
Schema definition system for data generation.
"""
import json
from typing import Dict, List, Any, Optional, Union, Callable
from pathlib import Path

class SchemaDefinition:
    """Schema definition for data generation."""
    
    def __init__(self, schema: Optional[Dict[str, Any]] = None, schema_path: Optional[str] = None):
        """
        Initialize schema definition.
        
        Args:
            schema: Dictionary containing schema definition
            schema_path: Path to a JSON file containing schema definition
        """
        if schema is not None:
            self.schema = schema
        elif schema_path is not None:
            self.schema = self._load_schema(schema_path)
        else:
            self.schema = {"fields": {}}
    
    def _load_schema(self, path: str) -> Dict[str, Any]:
        """Load schema from a JSON file."""
        with open(path, 'r') as f:
            return json.load(f)
    
    def save_schema(self, path: str) -> None:
        """Save schema to a JSON file."""
        with open(path, 'w') as f:
            json.dump(self.schema, f, indent=2)
    
    def add_field(self, name: str, field_def: Dict[str, Any]) -> None:
        """
        Add a field to the schema.
        
        Args:
            name: Field name
            field_def: Field definition dictionary
        """
        if "fields" not in self.schema:
            self.schema["fields"] = {}
        
        self.schema["fields"][name] = field_def
    
    def add_numeric_field(self, name: str, field_type: str = "integer", 
                          min_val: Union[int, float] = 0, max_val: Union[int, float] = 100,
                          distribution: str = "uniform", **kwargs) -> None:
        """
        Add a numeric field to the schema.
        
        Args:
            name: Field name
            field_type: One of "integer", "float", "decimal"
            min_val: Minimum value
            max_val: Maximum value
            distribution: Distribution type ("uniform", "normal")
            **kwargs: Additional field properties
        """
        field_def = {
            "type": field_type,
            "min": min_val,
            "max": max_val,
            "distribution": distribution,
            **kwargs
        }
        self.add_field(name, field_def)
    
    def add_text_field(self, name: str, min_length: int = 1, max_length: int = 50,
                      pattern: Optional[str] = None, values: Optional[List[str]] = None,
                      **kwargs) -> None:
        """
        Add a text field to the schema.
        
        Args:
            name: Field name
            min_length: Minimum text length
            max_length: Maximum text length
            pattern: Regex pattern for text generation
            values: List of possible values to select from
            **kwargs: Additional field properties
        """
        field_def = {
            "type": "string",
            "min_length": min_length,
            "max_length": max_length,
            **kwargs
        }
        
        if pattern is not None:
            field_def["pattern"] = pattern
        
        if values is not None:
            field_def["values"] = values
        
        self.add_field(name, field_def)
    
    def add_categorical_field(self, name: str, categories: List[str], 
                             weights: Optional[List[float]] = None, **kwargs) -> None:
        """
        Add a categorical field to the schema.
        
        Args:
            name: Field name
            categories: List of category values
            weights: Optional list of weights for sampling categories
            **kwargs: Additional field properties
        """
        field_def = {
            "type": "categorical",
            "categories": categories,
            **kwargs
        }
        
        if weights is not None:
            field_def["weights"] = weights
        
        self.add_field(name, field_def)
    
    def add_temporal_field(self, name: str, field_type: str = "date",
                          start_date: str = "2020-01-01", end_date: str = "2023-12-31",
                          format: Optional[str] = None, **kwargs) -> None:
        """
        Add a temporal field to the schema.
        
        Args:
            name: Field name
            field_type: One of "date", "time", "datetime", "timestamp"
            start_date: Start date/time
            end_date: End date/time
            format: Format string for date/time
            **kwargs: Additional field properties
        """
        field_def = {
            "type": field_type,
            "start": start_date,
            "end": end_date,
            **kwargs
        }
        
        if format is not None:
            field_def["format"] = format
        
        self.add_field(name, field_def)
    
    def validate(self) -> bool:
        """
        Validate the schema definition.
        
        Returns:
            True if schema is valid
        """
        # Basic validation
        if "fields" not in self.schema:
            raise ValueError("Schema must contain 'fields' property")
            
        if not isinstance(self.schema["fields"], dict):
            raise ValueError("'fields' must be a dictionary")
            
        if len(self.schema["fields"]) == 0:
            raise ValueError("Schema must contain at least one field")
        
        # Validate each field
        for field_name, field_def in self.schema["fields"].items():
            if "type" not in field_def:
                raise ValueError(f"Field {field_name} is missing 'type' property")
        
        return True