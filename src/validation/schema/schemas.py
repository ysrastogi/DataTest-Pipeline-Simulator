from pyspark.sql.types import *
from typing import Dict, Any, List, Optional

class SchemaRegistry:
    """
    Registry to manage and provide access to schemas.
    """
    
    def __init__(self):
        self.schemas = {}
        
    def register_schema(self, name: str, schema: StructType) -> None:
        """Register a schema with a name."""
        self.schemas[name] = schema
        
    def get_schema(self, name: str) -> Optional[StructType]:
        """Get a schema by name."""
        return self.schemas.get(name)
    
    def has_schema(self, name: str) -> bool:
        """Check if a schema exists."""
        return name in self.schemas
    
    def register_dict_schema(self, name: str, schema_dict: Dict[str, Any]) -> None:
        """
        Register a schema from a dictionary specification.
        
        Args:
            name: Name to register the schema under
            schema_dict: Dictionary with format {'field_name': 'data_type', ...}
                where data_type is a string like 'string', 'integer', etc.
        """
        fields = []
        
        type_mapping = {
            'string': StringType(),
            'integer': IntegerType(),
            'int': IntegerType(),
            'long': LongType(),
            'double': DoubleType(),
            'float': FloatType(),
            'boolean': BooleanType(),
            'bool': BooleanType(),
            'date': DateType(),
            'timestamp': TimestampType(),
            'binary': BinaryType()
        }
        
        for field_name, field_spec in schema_dict.items():
            if isinstance(field_spec, str):
                # Simple type specification
                data_type = type_mapping.get(field_spec.lower())
                if not data_type:
                    raise ValueError(f"Unknown data type: {field_spec}")
                
                fields.append(StructField(field_name, data_type, True))
            elif isinstance(field_spec, dict):
                # Advanced specification with nullability and possibly nested types
                data_type_str = field_spec.get('type', 'string').lower()
                nullable = field_spec.get('nullable', True)
                
                data_type = type_mapping.get(data_type_str)
                if not data_type:
                    raise ValueError(f"Unknown data type: {data_type_str}")
                
                fields.append(StructField(field_name, data_type, nullable))
        
        self.schemas[name] = StructType(fields)

# Create a global schema registry instance
schema_registry = SchemaRegistry()

# Register common schemas
schema_registry.register_dict_schema(
    'user', 
    {
        'user_id': 'string',
        'name': 'string',
        'age': 'integer',
        'email': 'string',
        'country': 'string',
        'registration_date': 'date'
    }
)