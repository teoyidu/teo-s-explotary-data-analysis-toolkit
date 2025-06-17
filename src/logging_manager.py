import logging
import logging.handlers
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from rich.logging import RichHandler
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
import psutil
import threading
from queue import Queue
import json
import os
import gc
from dataclasses import dataclass, asdict
from collections import deque

@dataclass
class PerformanceMetrics:
    """Structured performance metrics"""
    operation: str
    duration_seconds: float
    memory_usage_mb: float
    cpu_percent: float
    io_counters: Dict[str, int]
    gc_stats: Dict[str, int]
    timestamp: str
    success: bool
    error_message: Optional[str] = None
    additional_metrics: Optional[Dict[str, Any]] = None

class LoggingManager:
    def __init__(
        self,
        log_dir: str = "logs",
        log_level: int = logging.INFO,
        max_log_size: int = 100 * 1024 * 1024,  # 100MB
        backup_count: int = 5,
        metrics_history_size: int = 1000
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
        self.metrics_history = deque(maxlen=metrics_history_size)
        self.metrics_thread = threading.Thread(target=self._process_metrics, daemon=True)
        self.metrics_thread.start()
        
        # System monitoring
        self.process = psutil.Process()
        self.system_monitor = threading.Thread(target=self._monitor_system_resources, daemon=True)
        self.system_monitor.start()
        
        # Initialize performance tracking
        self._initialize_performance_tracking()
        
    def _initialize_performance_tracking(self):
        """Initialize performance tracking structures"""
        self.operation_stats = {
            'total_operations': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'total_duration': 0.0,
            'average_duration': 0.0,
            'max_duration': 0.0,
            'min_duration': float('inf'),
            'memory_peaks': [],
            'cpu_peaks': []
        }
        
    def _get_io_counters(self) -> Dict[str, int]:
        """Safely get IO counters with error handling"""
        try:
            counters = getattr(self.process, 'io_counters', lambda: None)()
            if counters is None:
                return {
                    'read_count': 0,
                    'write_count': 0,
                    'read_bytes': 0,
                    'write_bytes': 0
                }
            return {
                'read_count': counters.read_count,
                'write_count': counters.write_count,
                'read_bytes': counters.read_bytes,
                'write_bytes': counters.write_bytes
            }
        except (psutil.AccessDenied, psutil.ZombieProcess, AttributeError):
            # Return default values if IO counters are not available
            return {
                'read_count': 0,
                'write_count': 0,
                'read_bytes': 0,
                'write_bytes': 0
            }
        
    def _monitor_system_resources(self):
        """Background thread to monitor system resources"""
        while True:
            try:
                memory_info = self.process.memory_info()
                cpu_percent = self.process.cpu_percent()
                io_counters = self._get_io_counters()
                
                self.operation_stats['memory_peaks'].append(memory_info.rss / 1024 / 1024)  # MB
                self.operation_stats['cpu_peaks'].append(cpu_percent)
                
                # Keep only last 100 measurements
                if len(self.operation_stats['memory_peaks']) > 100:
                    self.operation_stats['memory_peaks'].pop(0)
                if len(self.operation_stats['cpu_peaks']) > 100:
                    self.operation_stats['cpu_peaks'].pop(0)
                
                time.sleep(1)  # Monitor every second
                
            except Exception as e:
                self.logger.error(f"Error in system monitoring: {e}")
                time.sleep(5)  # Wait longer on error
                
    def _process_metrics(self):
        """Background thread to process and log metrics"""
        while True:
            try:
                metric = self.metrics_queue.get()
                if metric is None:
                    break
                    
                # Convert to PerformanceMetrics if it's a dict
                if isinstance(metric, dict):
                    metric = PerformanceMetrics(**metric)
                    
                # Update operation statistics
                self.operation_stats['total_operations'] += 1
                if metric.success:
                    self.operation_stats['successful_operations'] += 1
                else:
                    self.operation_stats['failed_operations'] += 1
                    
                self.operation_stats['total_duration'] += metric.duration_seconds
                self.operation_stats['average_duration'] = (
                    self.operation_stats['total_duration'] / self.operation_stats['total_operations']
                )
                self.operation_stats['max_duration'] = max(
                    self.operation_stats['max_duration'],
                    metric.duration_seconds
                )
                self.operation_stats['min_duration'] = min(
                    self.operation_stats['min_duration'],
                    metric.duration_seconds
                )
                
                # Store in history
                self.metrics_history.append(metric)
                
                # Log the metric
                self._log_metric(metric)
                
            except Exception as e:
                self.logger.error(f"Error processing metric: {e}")
                
    def _log_metric(self, metric: PerformanceMetrics):
        """Log a performance metric with detailed information"""
        metric_dict = asdict(metric)
        self.logger.info(f"PERFORMANCE_METRIC: {json.dumps(metric_dict)}")
        
    def start_operation(self, operation_name: str):
        """Start timing an operation"""
        self.start_times[operation_name] = time.time()
        self.logger.info(f"Starting operation: {operation_name}")
        
    def end_operation(
        self,
        operation_name: str,
        success: bool = True,
        error_message: Optional[str] = None,
        additional_metrics: Optional[Dict[str, Any]] = None
    ):
        """End timing an operation and log detailed metrics"""
        if operation_name not in self.start_times:
            self.logger.warning(f"No start time found for operation: {operation_name}")
            return
            
        duration = time.time() - self.start_times[operation_name]
        
        # Collect system metrics
        memory_info = self.process.memory_info()
        cpu_percent = self.process.cpu_percent()
        io_counters = self._get_io_counters()
        gc_stats = gc.get_stats()
        
        # Create performance metrics
        metric = PerformanceMetrics(
            operation=operation_name,
            duration_seconds=duration,
            memory_usage_mb=memory_info.rss / 1024 / 1024,
            cpu_percent=cpu_percent,
            io_counters=io_counters,
            gc_stats={
                'collections': sum(stat['collections'] for stat in gc_stats),
                'collected': sum(stat['collected'] for stat in gc_stats)
            },
            timestamp=datetime.now().isoformat(),
            success=success,
            error_message=error_message,
            additional_metrics=additional_metrics
        )
        
        self.metrics_queue.put(metric)
        self.logger.info(
            f"Completed operation: {operation_name} in {duration:.2f} seconds "
            f"(Memory: {memory_info.rss / 1024 / 1024:.1f}MB, "
            f"CPU: {cpu_percent:.1f}%)"
        )
        del self.start_times[operation_name]
        
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get a summary of performance metrics"""
        return {
            'operation_stats': self.operation_stats,
            'recent_metrics': list(self.metrics_history),
            'system_stats': {
                'memory_usage_mb': self.process.memory_info().rss / 1024 / 1024,
                'cpu_percent': self.process.cpu_percent(),
                'thread_count': self.process.num_threads(),
                'open_files': len(self.process.open_files()),
                'connections': len(self.process.connections())
            }
        }
        
    def log_batch_progress(
        self,
        batch_num: int,
        total_batches: int,
        processed_items: int,
        total_items: int,
        batch_metrics: Optional[Dict[str, Any]] = None
    ):
        """Log progress of batch processing with detailed metrics"""
        progress = (processed_items / total_items) * 100
        self.logger.info(
            f"Batch {batch_num}/{total_batches} - "
            f"Processed {processed_items}/{total_items} items "
            f"({progress:.2f}%)"
        )
        
        if batch_metrics:
            self.logger.info(f"Batch metrics: {json.dumps(batch_metrics)}")
            
    def log_error(self, error: Exception, context: Optional[Dict[str, Any]] = None):
        """Log an error with detailed context and stack trace"""
        error_info = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context or {},
            "timestamp": datetime.now().isoformat(),
            "system_info": {
                "memory_usage_mb": self.process.memory_info().rss / 1024 / 1024,
                "cpu_percent": self.process.cpu_percent(),
                "thread_count": self.process.num_threads()
            }
        }
        self.logger.error(f"ERROR: {json.dumps(error_info)}", exc_info=True)
        
    def log_accuracy_metrics(self, metrics: Dict[str, Any]):
        """Log accuracy metrics for an operation"""
        self.logger.info(f"ACCURACY_METRICS: {json.dumps(metrics)}")
        
    def cleanup(self):
        """Cleanup resources and generate final report"""
        # Stop monitoring threads
        self.metrics_queue.put(None)
        self.metrics_thread.join()
        
        # Generate final performance report
        performance_summary = self.get_performance_summary()
        self.logger.info(f"FINAL_PERFORMANCE_REPORT: {json.dumps(performance_summary)}")
        
        # Close handlers
        for handler in self.logger.handlers:
            handler.close() 