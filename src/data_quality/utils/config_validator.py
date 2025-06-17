"""
Configuration validation utility for the Data Quality Framework
"""

import jsonschema
from jsonschema import validate, ValidationError
from typing import Dict, List, Tuple, Any

class ConfigurationValidator:
    """Validates configuration for the data quality framework"""
    
    CONFIG_SCHEMA = {
        "type": "object",
        "required": ["checkpoint_dir", "output_dir", "batch_size"],
        "properties": {
            "checkpoint_dir": {"type": "string"},
            "output_dir": {"type": "string"},
            "batch_size": {"type": "integer", "minimum": 1},
            "missing_value_strategy": {
                "type": "string",
                "enum": ["drop", "fill"]
            },
            "missing_threshold": {
                "type": "number",
                "minimum": 0,
                "maximum": 100
            },
            "critical_columns": {
                "type": "array",
                "items": {"type": "string"}
            },
            "fill_values": {
                "type": "object",
                "additionalProperties": True
            },
            "mandatory_fields": {
                "type": "array",
                "items": {"type": "string"}
            },
            "numerical_columns": {
                "type": "array",
                "items": {"type": "string"}
            },
            "decimal_places": {
                "type": "integer",
                "minimum": 0
            },
            "date_columns": {
                "type": "array",
                "items": {"type": "string"}
            },
            "data_retention_days": {
                "type": "integer",
                "minimum": 1
            },
            "irrelevant_data_conditions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["column", "values"],
                    "properties": {
                        "column": {"type": "string"},
                        "values": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    }
                }
            },
            "reference_data": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["path", "key_column", "df_column"],
                    "properties": {
                        "path": {"type": "string"},
                        "key_column": {"type": "string"},
                        "df_column": {"type": "string"}
                    }
                }
            },
            "unique_constraints": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["columns"],
                    "properties": {
                        "columns": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "action": {
                            "type": "string",
                            "enum": ["drop_duplicates", "keep_first"]
                        },
                        "order_by": {"type": "string"}
                    }
                }
            },
            "category_mappings": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}
                }
            },
            "text_standardization": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "trim": {"type": "boolean"},
                        "case": {
                            "type": "string",
                            "enum": ["upper", "lower", "title"]
                        }
                    }
                }
            },
            "text_validation_rules": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "pattern": {"type": "string"},
                        "min_length": {"type": "integer", "minimum": 0},
                        "max_length": {"type": "integer", "minimum": 0},
                        "remove_special_chars": {"type": "boolean"}
                    }
                }
            },
            "relationship_rules": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["type"],
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["foreign_key", "conditional"]
                        },
                        "child_column": {"type": "string"},
                        "parent_table_path": {"type": "string"},
                        "parent_column": {"type": "string"},
                        "condition_column": {"type": "string"},
                        "condition_value": {"type": "boolean"},
                        "dependent_column": {"type": "string"},
                        "dependent_required": {"type": "boolean"}
                    }
                }
            },
            "entry_rules": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "type"],
                    "properties": {
                        "name": {"type": "string"},
                        "type": {
                            "type": "string",
                            "enum": ["range_check", "allowed_values", "cross_field_validation"]
                        },
                        "column": {"type": "string"},
                        "min_value": {"type": "number"},
                        "max_value": {"type": "number"},
                        "allowed_values": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "field1": {"type": "string"},
                        "field2": {"type": "string"},
                        "operator": {
                            "type": "string",
                            "enum": [">", "<", ">=", "<=", "==", "!="]
                        }
                    }
                }
            }
        }
    }
    
    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate configuration against schema
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        try:
            validate(instance=config, schema=cls.CONFIG_SCHEMA)
            return True, []
        except ValidationError as e:
            return False, [str(e)]
        except Exception as e:
            return False, [f"Unexpected error during validation: {str(e)}"] 