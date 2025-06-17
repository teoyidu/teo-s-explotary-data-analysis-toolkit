#!/usr/bin/env python3
"""
Centralized logging configuration for the Data Quality Framework
"""

import logging
import os
from datetime import datetime

def setup_logging(log_dir: str = 'logs', log_level: int = logging.INFO) -> logging.Logger:
    """
    Set up centralized logging configuration
    
    Args:
        log_dir: Directory to store log files
        log_level: Logging level
        
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Generate log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'data_quality_{timestamp}.log')
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    # Create and return logger
    logger = logging.getLogger('data_quality')
    return logger 