"""
Data Quality Framework Package

A comprehensive data validation and cleansing pipeline for PySpark DataFrames.
"""

# Import core components
from .core.framework import DataQualityFramework
from .core.exceptions import (
    DataQualityError,
    ConfigurationError,
    ValidationError,
    ProcessingError,
    ExternalValidationError
)
from .exceptions import ModelLoadError, InferenceError

# Import utility functions
from .utils.config_validator import ConfigurationValidator
from .utils.metrics import MetricsCollector
from .utils.legal_domain_filter import LegalDomainFilter

# Import processor factory
from .processors import get_processor

__version__ = "1.0.0"

__all__ = [
    # Core framework
    'DataQualityFramework',
    
    # Exceptions
    'DataQualityError',
    'ConfigurationError', 
    'ValidationError',
    'ProcessingError',
    'ExternalValidationError',
    'ModelLoadError',
    'InferenceError',
    
    # Utilities
    'ConfigurationValidator',
    'MetricsCollector',
    'LegalDomainFilter',
    
    # Processor factory
    'get_processor',
    
    # Version
    '__version__'
] 