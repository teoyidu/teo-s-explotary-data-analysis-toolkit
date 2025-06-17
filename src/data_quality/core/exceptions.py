"""
Custom exceptions for the Data Quality Framework
"""

class DataQualityError(Exception):
    """Base exception for data quality framework errors"""
    pass

class ConfigurationError(DataQualityError):
    """Raised when there are configuration issues"""
    pass

class ValidationError(DataQualityError):
    """Raised when data validation fails"""
    pass

class ProcessingError(DataQualityError):
    """Raised when data processing fails"""
    pass

class ExternalValidationError(DataQualityError):
    """Raised when external validation fails"""
    pass 