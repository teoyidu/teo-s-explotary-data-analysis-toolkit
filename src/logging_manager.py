import logging
import logging.handlers
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from rich.logging import RichHandler
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
import psutil
import threading
from queue import Queue
import json

class LoggingManager:
    def __init__(
        self,
        log_dir: str = "logs",
        log_level: int = logging.INFO,
        max_log_size: int = 100 * 1024 * 1024,  # 100MB
        backup_count: int = 5
    ):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Initialize rich console
        self.console = Console()
        
        # Set up file handler with rotation
        self.file_handler = logging.handlers.RotatingFileHandler(
            self.log_dir / "processing.log",
            maxBytes=max_log_size,
            backupCount=backup_count
        )
        
        # Set up rich console handler
        self.console_handler = RichHandler(rich_tracebacks=True)
        
        # Configure logger with a unique name to prevent propagation
        self.logger = logging.getLogger(f"data_processor_{id(self)}")
        self.logger.propagate = False  # Prevent propagation to parent loggers
        self.logger.setLevel(log_level)
        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.console_handler)
        
        # Performance tracking
        self.start_times: Dict[str, float] = {}
        self.metrics_queue = Queue()
        self.metrics_thread = threading.Thread(target=self._process_metrics, daemon=True)
        self.metrics_thread.start()
        
        # Memory tracking
        self.process = psutil.Process()
        
    def _process_metrics(self):
        """Background thread to process and log metrics"""
        while True:
            try:
                metric = self.metrics_queue.get()
                if metric is None:
                    break
                self._log_metric(metric)
            except Exception as e:
                self.logger.error(f"Error processing metric: {e}")
                
    def _log_metric(self, metric: Dict[str, Any]):
        """Log a metric with memory usage and timestamp"""
        memory_info = self.process.memory_info()
        metric.update({
            "timestamp": datetime.now().isoformat(),
            "memory_rss": memory_info.rss / 1024 / 1024,  # MB
            "memory_vms": memory_info.vms / 1024 / 1024,  # MB
        })
        self.logger.info(f"METRIC: {json.dumps(metric)}")
        
    def start_operation(self, operation_name: str):
        """Start timing an operation"""
        self.start_times[operation_name] = time.time()
        self.logger.info(f"Starting operation: {operation_name}")
        
    def end_operation(self, operation_name: str, accuracy: Optional[float] = None, 
                     additional_metrics: Optional[Dict[str, Any]] = None):
        """End timing an operation and log metrics"""
        if operation_name not in self.start_times:
            self.logger.warning(f"No start time found for operation: {operation_name}")
            return
            
        duration = time.time() - self.start_times[operation_name]
        metrics = {
            "operation": operation_name,
            "duration_seconds": duration,
            "accuracy": accuracy
        }
        
        if additional_metrics:
            metrics.update(additional_metrics)
            
        self.metrics_queue.put(metrics)
        self.logger.info(f"Completed operation: {operation_name} in {duration:.2f} seconds")
        del self.start_times[operation_name]
        
    def log_batch_progress(self, batch_num: int, total_batches: int, 
                          processed_items: int, total_items: int):
        """Log progress of batch processing"""
        progress = (processed_items / total_items) * 100
        self.logger.info(
            f"Batch {batch_num}/{total_batches} - "
            f"Processed {processed_items}/{total_items} items "
            f"({progress:.2f}%)"
        )
        
    def log_error(self, error: Exception, context: Optional[Dict[str, Any]] = None):
        """Log an error with context"""
        error_info = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context or {}
        }
        self.logger.error(f"ERROR: {json.dumps(error_info)}", exc_info=True)
        
    def log_accuracy_metrics(self, metrics: Dict[str, float]):
        """Log accuracy-related metrics"""
        self.logger.info(f"ACCURACY_METRICS: {json.dumps(metrics)}")
        
    def cleanup(self):
        """Cleanup resources"""
        self.metrics_queue.put(None)  # Signal metrics thread to stop
        self.metrics_thread.join()
        for handler in self.logger.handlers:
            handler.close() 