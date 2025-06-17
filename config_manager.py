#!/usr/bin/env python3
"""
Configuration Manager for Data Quality Framework
Handles loading, validation, and management of configuration settings
"""

import json
import yaml
import os
from typing import Dict, Any, List, Optional, Tuple
import logging
from dataclasses import dataclass, field
from datetime import datetime
import jsonschema
from jsonschema import validate, ValidationError

logger = logging.getLogger(__name__)

# Configuration schema
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
                    "type": {"type": "string"},
                    "source_column": {"type": "string"},
                    "target_column": {"type": "string"},
                    "condition": {"type": "string"}
                }
            }
        },
        "entry_rules": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["column", "condition"],
                "properties": {
                    "column": {"type": "string"},
                    "condition": {"type": "string"},
                    "action": {
                        "type": "string",
                        "enum": ["reject", "warn", "transform"]
                    }
                }
            }
        },
        "html_columns": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "handle_entities": {"type": "boolean"}
                }
            }
        },
        "hadoop_columns": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "remove_metadata": {"type": "boolean"}
                }
            }
        },
        "boilerplate_columns": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "remove_duplicates": {"type": "boolean"},
                    "remove_header_footer": {"type": "boolean"},
                    "custom_patterns": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                }
            }
        }
    }
}

@dataclass
class DataQualityConfig:
    """Data class for data quality configuration"""
    
    # General settings
    checkpoint_dir: str = '/tmp/dq_checkpoints'
    output_dir: str = '/tmp/dq_output'
    batch_size: int = 1000000
    
    # F1: Missing Values
    missing_value_strategy: str = 'drop'
    missing_threshold: float = 50.0
    critical_columns: List[str] = field(default_factory=list)
    fill_values: Dict[str, Any] = field(default_factory=dict)
    
    # F2: Mandatory Fields
    mandatory_fields: List[str] = field(default_factory=list)
    
    # F3: Numerical Standardization
    numerical_columns: List[str] = field(default_factory=list)
    decimal_places: int = 2
    
    # F4: Outdated Data
    date_columns: List[str] = field(default_factory=list)
    data_retention_days: int = 365
    irrelevant_data_conditions: List[Dict[str, Any]] = field(default_factory=list)
    
    # F5: External Source Validation
    reference_data: List[Dict[str, str]] = field(default_factory=list)
    
    # F6: Uniqueness
    unique_constraints: List[Dict[str, Any]] = field(default_factory=list)
    
    # F7: Category Mappings
    category_mappings: Dict[str, Dict[str, str]] = field(default_factory=dict)
    text_standardization: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # F8: Text Validation
    text_validation_rules: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # F9: Relationships
    relationship_rules: List[Dict[str, Any]] = field(default_factory=list)
    
    # F10: Entry Rules
    entry_rules: List[Dict[str, Any]] = field(default_factory=list)
    
    # F11: HTML Cleaning
    html_columns: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # F12: Hadoop Cleaning
    hadoop_columns: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # F13: Boilerplate Cleaning
    boilerplate_columns: Dict[str, Dict[str, Any]] = field(default_factory=dict)

class ConfigurationManager:
    """Manager for handling data quality configuration"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        self.config = DataQualityConfig()
        if config_path:
            self.load_config(config_path)
            
    def merge_config(self, new_config: Dict[str, Any]) -> None:
        """
        Merge new configuration with existing configuration
        
        Args:
            new_config: New configuration to merge
        """
        current_config = self.to_dict()
        merged_config = {**current_config, **new_config}
        
        # Validate merged configuration
        is_valid, errors = self.validate_config(merged_config)
        if not is_valid:
            raise ValueError(f"Invalid merged configuration: {', '.join(errors)}")
            
        # Update configuration
        self._update_config_from_dict(merged_config)
    
    def load_config(self, config_path: str) -> None:
        """
        Load configuration from file
        
        Args:
            config_path: Path to configuration file
        """
        try:
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
            with open(config_path, 'r') as f:
                if config_path.endswith('.json'):
                    config_data = json.load(f)
                elif config_path.endswith(('.yml', '.yaml')):
                    config_data = yaml.safe_load(f)
                else:
                    raise ValueError(f"Unsupported configuration file format: {config_path}")
            
            # Validate configuration
            is_valid, errors = self.validate_config(config_data)
            if not is_valid:
                raise ValueError(f"Invalid configuration: {', '.join(errors)}")
            
            # Update configuration
            self._update_config_from_dict(config_data)
            logger.info(f"Configuration loaded from: {config_path}")
            
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise
    
    def validate_config(self, config: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate configuration against schema
        
        Args:
            config: Configuration to validate
            
        Returns:
            Tuple of (is_valid, list of errors)
        """
        try:
            validate(instance=config, schema=CONFIG_SCHEMA)
            return True, []
        except ValidationError as e:
            return False, [str(e)]
    
    def _update_config_from_dict(self, config_data: Dict[str, Any]) -> None:
        """Update configuration object from dictionary"""
        for key, value in config_data.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
            else:
                logger.warning(f"Unknown configuration key: {key}")
    
    def save_config(self, output_path: str, format: str = 'json') -> None:
        """
        Save current configuration to file
        
        Args:
            output_path: Path to save configuration
            format: Output format ('json' or 'yaml')
        """
        try:
            config_dict = self.to_dict()
            
            # Validate before saving
            is_valid, errors = self.validate_config(config_dict)
            if not is_valid:
                raise ValueError(f"Invalid configuration: {', '.join(errors)}")
            
            with open(output_path, 'w') as f:
                if format.lower() == 'json':
                    json.dump(config_dict, f, indent=2, default=str)
                elif format.lower() in ['yaml', 'yml']:
                    yaml.dump(config_dict, f, default_flow_style=False)
                else:
                    raise ValueError(f"Unsupported format: {format}")
            
            logger.info(f"Configuration saved to: {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving configuration: {str(e)}")
            raise
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        config_dict = {}
        for field_name in self.config.__dataclass_fields__:
            config_dict[field_name] = getattr(self.config, field_name)
        return config_dict
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """
        Get validation summary of current configuration
        
        Returns:
            Dictionary with validation summary
        """
        config_dict = self.to_dict()
        is_valid, errors = self.validate_config(config_dict)
        
        return {
            'is_valid': is_valid,
            'errors': errors,
            'config_path': self.config_path,
            'last_updated': datetime.now().isoformat()
        }


def create_sample_configurations():
    """Create sample configuration files for different use cases"""
    
    # E-commerce configuration
    ecommerce_config = {
        'checkpoint_dir': '/data/ecommerce/checkpoints',
        'output_dir': '/data/ecommerce/cleaned',
        'batch_size': 2000000,
        
        'missing_value_strategy': 'fill',
        'missing_threshold': 30.0,
        'critical_columns': ['order_id', 'customer_id', 'product_id', 'order_date'],
        'fill_values': {
            'discount': 0.0,
            'shipping_cost': 0.0,
            'customer_rating': 3.0
        },
        
        'mandatory_fields': ['order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'price'],
        
        'numerical_columns': ['price', 'quantity', 'discount', 'shipping_cost', 'tax_amount'],
        'decimal_places': 2,
        
        'date_columns': ['order_date', 'delivery_date'],
        'data_retention_days': 2555,  # 7 years
        
        'unique_constraints': [
            {
                'columns': ['order_id'],
                'action': 'drop_duplicates'
            }
        ],
        
        'category_mappings': {
            'order_status': {
                'P': 'Pending',
                'S': 'Shipped',
                'D': 'Delivered',
                'C': 'Cancelled'
            }
        },
        
        'text_validation_rules': {
            'email': {
                'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
                'min_length': 5,
                'max_length': 100
            }
        },
        
        'entry_rules': [
            {
                'name': 'price_validation',
                'type': 'range_check',
                'column': 'price',
                'min_value': 0.01,
                'max_value': 100000.0
            },
            {
                'name': 'quantity_validation',
                'type': 'range_check',
                'column': 'quantity',
                'min_value': 1,
                'max_value': 1000
            }
        ]
    }
    
    # Financial services configuration
    financial_config = {
        'checkpoint_dir': '/data/financial/checkpoints',
        'output_dir': '/data/financial/cleaned',
        'batch_size': 1000000,
        
        'missing_value_strategy': 'drop',
        'missing_threshold': 5.0,  # Strict for financial data
        'critical_columns': ['transaction_id', 'account_id', 'amount', 'transaction_date'],
        
        'mandatory_fields': ['transaction_id', 'account_id', 'amount', 'transaction_date', 'transaction_type'],
        
        'numerical_columns': ['amount', 'balance', 'credit_limit'],
        'decimal_places': 2,
        
        'date_columns': ['transaction_date', 'settlement_date'],
        'data_retention_days': 2555,  # 7 years for compliance
        
        'unique_constraints': [
            {
                'columns': ['transaction_id'],
                'action': 'drop_duplicates'
            }
        ],
        
        'entry_rules': [
            {
                'name': 'amount_validation',
                'type': 'range_check',
                'column': 'amount',
                'min_value': 0.01,
                'max_value': 1000000.0
            },
            {
                'name': 'valid_transaction_types',
                'type': 'allowed_values',
                'column': 'transaction_type',
                'allowed_values': ['DEBIT', 'CREDIT', 'TRANSFER', 'FEE']
            }
        ]
    }
    
    # Healthcare configuration
    healthcare_config = {
        'checkpoint_dir': '/data/healthcare/checkpoints',
        'output_dir': '/data/healthcare/cleaned',
        'batch_size': 500000,
        
        'missing_value_strategy': 'fill',
        'missing_threshold': 20.0,
        'critical_columns': ['patient_id', 'visit_date', 'provider_id'],
        'fill_values': {
            'age': 0,
            'weight': 0.0,
            'blood_pressure_systolic': 120,
            'blood_pressure_diastolic': 80
        },
        
        'mandatory_fields': ['patient_id', 'visit_date', 'provider_id'],
        
        'numerical_columns': ['age', 'weight', 'height', 'blood_pressure_systolic', 'blood_pressure_diastolic'],
        'decimal_places': 1,
        
        'date_columns': ['visit_date', 'birth_date'],
        'data_retention_days': 2555,  # 7 years
        
        'text_validation_rules': {
            'patient_id': {
                'pattern': r'^P[0-9]{8}$',
                'min_length': 9,
                'max_length': 9
            }
        },
        
        'entry_rules': [
            {
                'name': 'age_validation',
                'type': 'range_check',
                'column': 'age',
                'min_value': 0,
                'max_value': 150
            },
            {
                'name': 'weight_validation',
                'type': 'range_check',
                'column': 'weight',
                'min_value': 0.5,
                'max_value': 500.0
            }
        ]
    }
    
    return {
        'ecommerce': ecommerce_config,
        'financial': financial_config,
        'healthcare': healthcare_config
    }


if __name__ == "__main__":
    # Example usage
    
    # Create configuration manager
    config_manager = ConfigurationManager()
    
    # Load sample configurations
    sample_configs = create_sample_configurations()
    
    # Create and save configurations for different domains
    for domain, config_data in sample_configs.items():
        domain_config = ConfigurationManager()
        domain_config.merge_config(config_data)
        
        # Validate configuration
        issues = domain_config.validate_config()
        if issues:
            print(f"Configuration issues for {domain}:")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print(f"Configuration for {domain} is valid")
        
        # Save configuration
        domain_config.save_config(f"{domain}_data_quality_config.json", "json")
        domain_config.save_config(f"{domain}_data_quality_config.yaml", "yaml")
        
        # Print summary
        summary = domain_config.get_validation_summary()
        print(f"\n{domain.title()} Configuration Summary:")
        for key, value in summary.items():
            print(f"  {key}: {value}")
        print("-" * 50) 