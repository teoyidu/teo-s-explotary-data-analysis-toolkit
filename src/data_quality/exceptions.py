"""
Custom exceptions for the data quality framework
"""

class DataQualityError(Exception):
    """Base exception for data quality framework errors"""
    pass

class ValidationError(DataQualityError):
    """Exception raised when validation fails"""
    pass

class ProcessingError(DataQualityError):
    """Exception raised when processing fails"""
    pass

class ConfigurationError(DataQualityError):
    """Exception raised when configuration is invalid"""
    pass

class LegalDomainError(DataQualityError):
    """Base exception for legal domain filtering errors"""
    pass

class ModelLoadError(LegalDomainError):
    """Exception raised when model fails to load"""
    pass

class InferenceError(LegalDomainError):
    """Exception raised when model inference fails"""
    pass 