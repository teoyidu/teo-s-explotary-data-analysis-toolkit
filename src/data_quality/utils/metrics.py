"""
Metrics collection utility for the Data Quality Framework
"""

import time
import psutil
import threading
from typing import Dict

class MetricsCollector:
    """Collects and manages performance metrics for the data quality framework"""
    
    def __init__(self):
        self.metrics = {
            'processing_times': {},
            'memory_usage': {},
            'record_counts': {},
            'validation_stats': {}
        }
        self._lock = threading.Lock()
    
    def start_timer(self, operation: str) -> float:
        """Start timing an operation"""
        return time.time()
    
    def end_timer(self, operation: str, start_time: float):
        """End timing an operation and record the duration"""
        duration = time.time() - start_time
        with self._lock:
            if operation not in self.metrics['processing_times']:
                self.metrics['processing_times'][operation] = []
            self.metrics['processing_times'][operation].append(duration)
    
    def record_memory_usage(self, operation: str):
        """Record memory usage for an operation"""
        memory_info = psutil.Process().memory_info()
        with self._lock:
            if operation not in self.metrics['memory_usage']:
                self.metrics['memory_usage'][operation] = []
            self.metrics['memory_usage'][operation].append({
                'rss': memory_info.rss,
                'vms': memory_info.vms,
                'shared': memory_info.shared,
                'text': memory_info.text,
                'data': memory_info.data
            })
    
    def record_record_count(self, operation: str, count: int):
        """Record the number of records processed in an operation"""
        with self._lock:
            if operation not in self.metrics['record_counts']:
                self.metrics['record_counts'][operation] = []
            self.metrics['record_counts'][operation].append(count)
    
    def record_validation_stats(self, operation: str, stats: Dict):
        """Record validation statistics for an operation"""
        with self._lock:
            if operation not in self.metrics['validation_stats']:
                self.metrics['validation_stats'][operation] = []
            self.metrics['validation_stats'][operation].append(stats)
    
    def get_metrics(self) -> Dict:
        """Get all collected metrics"""
        return self.metrics
    
    def get_summary(self) -> Dict:
        """Get a summary of the metrics"""
        summary = {
            'total_processing_time': sum(sum(times) for times in self.metrics['processing_times'].values()),
            'average_processing_times': {
                op: sum(times) / len(times) if times else 0
                for op, times in self.metrics['processing_times'].items()
            },
            'peak_memory_usage': {
                op: max(usage['rss'] for usage in usages) if usages else 0
                for op, usages in self.metrics['memory_usage'].items()
            },
            'total_records_processed': sum(sum(counts) for counts in self.metrics['record_counts'].values())
        }
        return summary 