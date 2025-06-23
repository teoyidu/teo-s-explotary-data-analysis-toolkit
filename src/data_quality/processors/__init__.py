"""
Data quality processors module
"""

def _get_processor_registry():
    """Lazy import function to avoid circular imports"""
    from .missing_values import MissingValuesProcessor
    from .mandatory_fields import MandatoryFieldsProcessor
    from .numerical_formats import NumericalFormatsProcessor
    from .outdated_data import OutdatedDataProcessor
    from .external_validation import ExternalValidationProcessor
    from .uniqueness import UniquenessProcessor
    from .categories import CategoriesProcessor
    from .text_validation import TextValidationProcessor
    from .relationships import RelationshipsProcessor
    from .entry_rules import EntryRulesProcessor
    from .html_cleaner import HTMLCleanerProcessor
    from .hadoop_cleaner import HadoopCleanerProcessor
    from .boilerplate_cleaner import BoilerplateCleanerProcessor
    from .duplicate_detector import DuplicateDetector, TurkishDuplicateDetector
    from .semhash_turkish_detector import SemHashTurkishDetector, SemHashDuplicateDetector
    
    return {
        'MissingValuesProcessor': MissingValuesProcessor,
        'MandatoryFieldsProcessor': MandatoryFieldsProcessor,
        'NumericalFormatsProcessor': NumericalFormatsProcessor,
        'OutdatedDataProcessor': OutdatedDataProcessor,
        'ExternalValidationProcessor': ExternalValidationProcessor,
        'UniquenessProcessor': UniquenessProcessor,
        'CategoriesProcessor': CategoriesProcessor,
        'TextValidationProcessor': TextValidationProcessor,
        'RelationshipsProcessor': RelationshipsProcessor,
        'EntryRulesProcessor': EntryRulesProcessor,
        'HTMLCleanerProcessor': HTMLCleanerProcessor,
        'HadoopCleanerProcessor': HadoopCleanerProcessor,
        'BoilerplateCleanerProcessor': BoilerplateCleanerProcessor,
        'DuplicateDetector': DuplicateDetector,
        'TurkishDuplicateDetector': TurkishDuplicateDetector,
        'SemHashTurkishDetector': SemHashTurkishDetector,
        'SemHashDuplicateDetector': SemHashDuplicateDetector
    }

def get_processor(processor_name: str):
    """Get a processor class by name"""
    registry = _get_processor_registry()
    if processor_name not in registry:
        raise ValueError(f"Unknown processor: {processor_name}")
    return registry[processor_name]

# For backward compatibility, provide direct access to commonly used processors
def _lazy_import_common_processors():
    """Lazy import for commonly used processors"""
    from .missing_values import MissingValuesProcessor
    from .mandatory_fields import MandatoryFieldsProcessor
    from .duplicate_detector import DuplicateDetector, TurkishDuplicateDetector
    from .semhash_turkish_detector import SemHashTurkishDetector, SemHashDuplicateDetector
    
    return (
        MissingValuesProcessor,
        MandatoryFieldsProcessor,
        DuplicateDetector,
        TurkishDuplicateDetector,
        SemHashTurkishDetector,
        SemHashDuplicateDetector
    )

# Export commonly used processors for backward compatibility
__all__ = [
    'MissingValuesProcessor',
    'MandatoryFieldsProcessor',
    'NumericalFormatsProcessor',
    'OutdatedDataProcessor',
    'ExternalValidationProcessor',
    'UniquenessProcessor',
    'CategoriesProcessor',
    'TextValidationProcessor',
    'RelationshipsProcessor',
    'EntryRulesProcessor',
    'HTMLCleanerProcessor',
    'HadoopCleanerProcessor',
    'BoilerplateCleanerProcessor',
    'DuplicateDetector',
    'TurkishDuplicateDetector',
    'SemHashTurkishDetector',
    'SemHashDuplicateDetector',
    'get_processor',
    '_get_processor_registry'
] 