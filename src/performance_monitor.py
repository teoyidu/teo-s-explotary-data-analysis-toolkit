"""
Performance monitoring and visualization for the Data Quality Framework
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from dataclasses import dataclass
import plotly.graph_objects as go
from plotly.subplots import make_subplots

@dataclass
class PerformanceReport:
    """Structured performance report"""
    timestamp: str
    operation_stats: Dict[str, Any]
    system_stats: Dict[str, Any]
    metrics_history: List[Dict[str, Any]]

class PerformanceMonitor:
    """Monitors and visualizes performance metrics"""
    
    def __init__(self, log_dir: str = "logs", output_dir: str = "performance_reports") -> None:
        """
        Initialize the performance monitor
        
        Args:
            log_dir: Directory containing log files
            output_dir: Directory for output reports and visualizations
        """
        self.log_dir = Path(log_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
    def generate_report(self, log_file: str) -> PerformanceReport:
        """
        Generate a performance report from log file
        
        Args:
            log_file: Name of the log file to analyze
            
        Returns:
            PerformanceReport object containing parsed metrics
        """
        metrics: List[Dict[str, Any]] = []
        operation_stats: Dict[str, Any] = {}
        system_stats: Dict[str, Any] = {}
        
        with open(self.log_dir / log_file, 'r') as f:
            for line in f:
                if "PERFORMANCE_METRIC:" in line:
                    metric = json.loads(line.split("PERFORMANCE_METRIC:")[1].strip())
                    metrics.append(metric)
                elif "FINAL_PERFORMANCE_REPORT:" in line:
                    report = json.loads(line.split("FINAL_PERFORMANCE_REPORT:")[1].strip())
                    operation_stats = report.get('operation_stats', {})
                    system_stats = report.get('system_stats', {})
        
        return PerformanceReport(
            timestamp=datetime.now().isoformat(),
            operation_stats=operation_stats,
            system_stats=system_stats,
            metrics_history=metrics
        )
    
    def plot_performance_metrics(self, report: PerformanceReport, output_file: Optional[str] = None) -> None:
        """
        Create performance visualization plots
        
        Args:
            report: PerformanceReport object containing metrics
            output_file: Optional path to save the visualization
        """
        # Convert metrics to DataFrame
        df = pd.DataFrame(report.metrics_history)
        
        # Create subplots
        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=(
                "Operation Duration", "Memory Usage",
                "CPU Usage", "I/O Operations",
                "GC Collections", "Success Rate"
            )
        )
        
        # Operation Duration
        fig.add_trace(
            go.Scatter(x=df['timestamp'], y=df['duration_seconds'], name="Duration"),
            row=1, col=1
        )
        
        # Memory Usage
        fig.add_trace(
            go.Scatter(x=df['timestamp'], y=df['memory_usage_mb'], name="Memory"),
            row=1, col=2
        )
        
        # CPU Usage
        fig.add_trace(
            go.Scatter(x=df['timestamp'], y=df['cpu_percent'], name="CPU"),
            row=2, col=1
        )
        
        # I/O Operations
        fig.add_trace(
            go.Scatter(x=df['timestamp'], y=df['io_counters.read_count'], name="Reads"),
            row=2, col=2
        )
        fig.add_trace(
            go.Scatter(x=df['timestamp'], y=df['io_counters.write_count'], name="Writes"),
            row=2, col=2
        )
        
        # GC Collections
        fig.add_trace(
            go.Scatter(x=df['timestamp'], y=df['gc_stats.collections'], name="Collections"),
            row=3, col=1
        )
        
        # Success Rate
        success_rate = df['success'].mean() * 100
        fig.add_trace(
            go.Indicator(
                mode="gauge+number",
                value=success_rate,
                title="Success Rate",
                gauge={'axis': {'range': [0, 100]}}
            ),
            row=3, col=2
        )
        
        # Update layout
        fig.update_layout(
            height=1200,
            width=1600,
            title_text="Performance Metrics Dashboard",
            showlegend=True
        )
        
        # Save or show plot
        if output_file:
            fig.write_html(self.output_dir / output_file)
        else:
            fig.show()
    
    def generate_summary_report(self, report: PerformanceReport, output_file: Optional[str] = None) -> str:
        """
        Generate a text summary of performance metrics
        
        Args:
            report: PerformanceReport object containing metrics
            output_file: Optional path to save the summary
            
        Returns:
            Formatted summary text
        """
        summary = [
            "Performance Summary Report",
            "=" * 50,
            f"Generated at: {report.timestamp}",
            "\nOperation Statistics:",
            f"- Total Operations: {report.operation_stats['total_operations']}",
            f"- Successful Operations: {report.operation_stats['successful_operations']}",
            f"- Failed Operations: {report.operation_stats['failed_operations']}",
            f"- Average Duration: {report.operation_stats['average_duration']:.2f} seconds",
            f"- Max Duration: {report.operation_stats['max_duration']:.2f} seconds",
            f"- Min Duration: {report.operation_stats['min_duration']:.2f} seconds",
            "\nSystem Statistics:",
            f"- Memory Usage: {report.system_stats['memory_usage_mb']:.1f} MB",
            f"- CPU Usage: {report.system_stats['cpu_percent']:.1f}%",
            f"- Thread Count: {report.system_stats['thread_count']}",
            f"- Open Files: {report.system_stats['open_files']}",
            f"- Active Connections: {report.system_stats['connections']}"
        ]
        
        summary_text = "\n".join(summary)
        
        if output_file:
            with open(self.output_dir / output_file, 'w') as f:
                f.write(summary_text)
        
        return summary_text
    
    def analyze_performance_trends(self, report: PerformanceReport) -> Dict[str, Any]:
        """
        Analyze performance trends and identify potential issues
        
        Args:
            report: PerformanceReport object containing metrics
            
        Returns:
            Dictionary containing trend analysis and potential issues
        """
        df = pd.DataFrame(report.metrics_history)
        
        analysis: Dict[str, Any] = {
            'memory_trend': {
                'increasing': df['memory_usage_mb'].diff().mean() > 0,
                'peak_usage': df['memory_usage_mb'].max(),
                'average_usage': df['memory_usage_mb'].mean()
            },
            'cpu_trend': {
                'high_usage_periods': len(df[df['cpu_percent'] > 80]),
                'average_usage': df['cpu_percent'].mean()
            },
            'io_trend': {
                'read_intensity': df['io_counters.read_count'].diff().mean(),
                'write_intensity': df['io_counters.write_count'].diff().mean()
            },
            'gc_trend': {
                'collection_frequency': df['gc_stats.collections'].diff().mean(),
                'total_collections': df['gc_stats.collections'].sum()
            },
            'operation_trend': {
                'success_rate': df['success'].mean() * 100,
                'average_duration': df['duration_seconds'].mean(),
                'duration_std': df['duration_seconds'].std()
            }
        }
        
        # Identify potential issues
        issues: List[str] = []
        
        if analysis['memory_trend']['increasing']:
            issues.append("Memory usage shows an increasing trend")
        if analysis['cpu_trend']['high_usage_periods'] > 0:
            issues.append(f"CPU usage exceeded 80% in {analysis['cpu_trend']['high_usage_periods']} periods")
        if analysis['operation_trend']['success_rate'] < 95:
            issues.append(f"Operation success rate is below 95% ({analysis['operation_trend']['success_rate']:.1f}%)")
        if analysis['operation_trend']['duration_std'] > analysis['operation_trend']['average_duration']:
            issues.append("High variation in operation duration")
            
        analysis['potential_issues'] = issues
        
        return analysis 