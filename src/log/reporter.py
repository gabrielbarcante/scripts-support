"""
Log report generator for JSONL log files.

This module provides utilities to parse and generate human-readable reports
from JSON Lines (.jsonl) log files created by the JSONFormatter.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Literal
from datetime import datetime
from collections import Counter, defaultdict


class LogReport:
    """
    Parse and generate reports from JSONL log files.
    
    Provides methods to create various reports including summaries,
    filtered views, statistics, and formatted outputs.
    """
    
    def __init__(self, log_file_path: str | Path):
        """
        Initialize the log reporter.
        
        Args:
            log_file_path: Path to the .jsonl log file.
            
        Raises:
            FileNotFoundError: If the log file doesn't exist.
            ValueError: If the file is not a .jsonl file.
        """
        self.log_file_path = Path(log_file_path)
        
        if not self.log_file_path.exists():
            raise FileNotFoundError(f"Log file not found: {self.log_file_path}")
        
        if self.log_file_path.suffix != ".jsonl":
            raise ValueError(f"Expected .jsonl file, got: {self.log_file_path.suffix}")
        
        self.log_entries: List[Dict[str, Any]] = []
        self._load_logs()
    
    def _load_logs(self) -> None:
        """Load and parse log entries from the JSONL file."""
        self.log_entries = []
        with open(self.log_file_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    self.log_entries.append(entry)
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping invalid JSON on line {line_num}: {e}")
    
    def summary(self) -> str:
        """
        Generate a summary report of the log file.
        
        Returns:
            A formatted string with log statistics and overview.
        """
        if not self.log_entries:
            return "No log entries found."
        
        total_entries = len(self.log_entries)
        level_counts = Counter(entry.get("level", "UNKNOWN") for entry in self.log_entries)
        
        # Time range
        timestamps = [entry["timestamp"] for entry in self.log_entries if "timestamp" in entry]
        if timestamps:
            first_log = min(timestamps)
            last_log = max(timestamps)
        else:
            first_log = last_log = "N/A"
        
        # Unique loggers
        loggers = set(entry.get("logger", "unknown") for entry in self.log_entries)
        
        report = []
        report.append("=" * 70)
        report.append("LOG SUMMARY REPORT")
        report.append("=" * 70)
        report.append(f"Log File: {self.log_file_path.name}")
        report.append(f"Total Entries: {total_entries}")
        report.append(f"Time Range: {first_log} to {last_log}")
        report.append(f"Unique Loggers: {len(loggers)}")
        report.append("")
        report.append("Log Level Distribution:")
        report.append("-" * 30)
        
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            count = level_counts.get(level, 0)
            percentage = (count / total_entries * 100) if total_entries > 0 else 0
            bar = "█" * int(percentage / 2)
            report.append(f"  {level:10} {count:5} ({percentage:5.1f}%) {bar}")
        
        # Other levels
        other_levels = {k: v for k, v in level_counts.items() 
                       if k not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]}
        for level, count in other_levels.items():
            percentage = (count / total_entries * 100) if total_entries > 0 else 0
            report.append(f"  {level:10} {count:5} ({percentage:5.1f}%)")
        
        report.append("=" * 70)
        return "\n".join(report)
    
    def filter_by_level(self, level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], limit: int | None = None) -> str:
        """
        Generate a report filtered by log level.
        
        Args:
            level: The log level to filter by.
            limit: Maximum number of entries to include (most recent first).
        
        Returns:
            A formatted string with filtered log entries.
        """
        filtered = [entry for entry in self.log_entries if entry.get("level") == level]
        
        if not filtered:
            return f"No {level} level entries found."
        
        if limit:
            filtered = filtered[-limit:]
        
        report = []
        report.append("=" * 70)
        report.append(f"{level} LEVEL LOGS ({len(filtered)} entries)")
        report.append("=" * 70)
        report.append("")
        
        for entry in filtered:
            report.append(self._format_entry(entry))
            report.append("-" * 70)
        
        return "\n".join(report)
    
    def filter_by_time_range(self, start_time: str | None = None, end_time: str | None = None) -> str:
        """
        Generate a report for logs within a time range.
        
        Args:
            start_time: ISO format timestamp (inclusive). If None, from beginning.
            end_time: ISO format timestamp (inclusive). If None, to end.
        
        Returns:
            A formatted string with filtered log entries.
        """
        filtered = self.log_entries
        
        if start_time:
            filtered = [e for e in filtered if e.get("timestamp", "") >= start_time]
        
        if end_time:
            filtered = [e for e in filtered if e.get("timestamp", "") <= end_time]
        
        if not filtered:
            return "No entries found in specified time range."
        
        report = []
        report.append("=" * 70)
        report.append(f"TIME-FILTERED LOGS ({len(filtered)} entries)")
        if start_time:
            report.append(f"Start: {start_time}")
        if end_time:
            report.append(f"End: {end_time}")
        report.append("=" * 70)
        report.append("")
        
        for entry in filtered:
            report.append(self._format_entry(entry))
            report.append("-" * 70)
        
        return "\n".join(report)
    
    def errors_and_criticals(self) -> str:
        """
        Generate a report of all ERROR and CRITICAL level logs.
        
        Returns:
            A formatted string with error and critical entries.
        """
        filtered = [
            entry for entry in self.log_entries 
            if entry.get("level") in ["ERROR", "CRITICAL"]
        ]
        
        if not filtered:
            return "No errors or critical issues found. ✓"
        
        report = []
        report.append("=" * 70)
        report.append(f"ERRORS AND CRITICAL ISSUES ({len(filtered)} entries)")
        report.append("=" * 70)
        report.append("")
        
        for entry in filtered:
            report.append(self._format_entry(entry, detailed=True))
            report.append("-" * 70)
        
        return "\n".join(report)
    
    def module_breakdown(self) -> str:
        """
        Generate a report breaking down logs by module.
        
        Returns:
            A formatted string with per-module statistics.
        """
        module_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        for entry in self.log_entries:
            module = entry.get("module", "unknown")
            level = entry.get("level", "UNKNOWN")
            module_stats[module][level] += 1
            module_stats[module]["total"] += 1
        
        report = []
        report.append("=" * 70)
        report.append("MODULE BREAKDOWN")
        report.append("=" * 70)
        report.append("")
        
        for module in sorted(module_stats.keys()):
            stats = module_stats[module]
            total = stats["total"]
            report.append(f"Module: {module} ({total} entries)")
            report.append("-" * 40)
            
            for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                count = stats.get(level, 0)
                if count > 0:
                    report.append(f"  {level:10} {count:5}")
            
            report.append("")
        
        return "\n".join(report)
    
    def full_report(self, limit: int | None = None) -> str:
        """
        Generate a complete detailed report of all logs.
        
        Args:
            limit: Maximum number of entries to include (most recent first).
        
        Returns:
            A formatted string with all log entries.
        """
        entries = self.log_entries
        
        if limit:
            entries = entries[-limit:]
        
        report = []
        report.append("=" * 70)
        report.append(f"FULL LOG REPORT ({len(entries)} entries)")
        report.append("=" * 70)
        report.append("")
        
        for entry in entries:
            report.append(self._format_entry(entry, detailed=True))
            report.append("-" * 70)
        
        return "\n".join(report)
    
    def _format_entry(self, entry: Dict[str, Any], detailed: bool = False) -> str:
        """
        Format a single log entry as a readable string.
        
        Args:
            entry: The log entry dictionary.
            detailed: Whether to include detailed metadata.
        
        Returns:
            A formatted string representation of the log entry.
        """
        lines = []
        
        # Header line
        timestamp = entry.get("timestamp", "N/A")
        level = entry.get("level", "UNKNOWN")
        message = entry.get("message", "")
        
        # Format timestamp for readability
        try:
            dt_obj = datetime.fromisoformat(timestamp)
            timestamp_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, AttributeError):
            timestamp_str = timestamp
        
        lines.append(f"[{timestamp_str}] {level:8} | {message}")
        
        if detailed:
            # Add location info
            logger = entry.get("logger", "")
            module = entry.get("module", "")
            function = entry.get("function", "")
            line = entry.get("line", "")
            
            if logger or module:
                location = f"Logger: {logger}, Module: {module}"
                if function:
                    location += f", Function: {function}"
                if line:
                    location += f", Line: {line}"
                lines.append(f"  └─ {location}")
            
            # Thread info if present
            thread_name = entry.get("thread_name", "")
            if thread_name:
                lines.append(f"  └─ Thread: {thread_name}")
            
            # Exception info if present
            if "exc_info" in entry:
                lines.append(f"  └─ Exception:")
                for exc_line in entry["exc_info"].split("\n"):
                    lines.append(f"     {exc_line}")
            
            # Stack info if present
            if "stack_info" in entry:
                lines.append(f"  └─ Stack Trace:")
                for stack_line in entry["stack_info"].split("\n"):
                    lines.append(f"     {stack_line}")
            
            # Custom fields
            standard_fields = {
                "level", "message", "timestamp", "logger", "module", 
                "function", "line", "thread_name", "exc_info", "stack_info"
            }
            custom_fields = {k: v for k, v in entry.items() if k not in standard_fields}
            if custom_fields:
                lines.append(f"  └─ Additional: {custom_fields}")
        
        return "\n".join(lines)
    
    def save_report(self, report_content: str, output_path: str | Path) -> None:
        """
        Save a report to a file.
        
        Args:
            report_content: The report content to save.
            output_path: Path where the report should be saved.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report_content)
        
        print(f"Report saved to: {output_path}")


def generate_report(log_file_path: str | Path, report_type: Literal["summary", "full", "errors", "modules"] = "summary", output_file: str | Path | None = None, **kwargs) -> str:
    """
    Convenience function to generate a report from a log file.
    
    Args:
        log_file_path: Path to the .jsonl log file to analyze.
        report_type: Type of report to generate. Options:
            - "summary": Overview with statistics and level distribution
            - "full": Complete detailed report of all log entries
            - "errors": Only ERROR and CRITICAL level logs
            - "modules": Breakdown of logs by module with statistics
        output_file: Optional path to save the report. If None, report is only returned.
        **kwargs: Additional arguments for specific report types:
            - limit (int): For "full" report, maximum number of entries (most recent first)
    
    Returns:
        The generated report as a formatted string.
    
    Raises:
        FileNotFoundError: If the log file doesn't exist.
        ValueError: If the file is not a .jsonl file or report_type is invalid.
    
    Examples:
        >>> # Generate and print a summary report
        >>> report = generate_report("logs/app.log.jsonl", "summary")
        >>> print(report)
        
        >>> # Generate full report with limit and save to file
        >>> generate_report(
        ...     "logs/app.log.jsonl", 
        ...     "full",
        ...     output_file="reports/full_report.txt",
        ...     limit=100
        ... )
        
        >>> # Generate errors-only report
        >>> errors = generate_report("logs/app.log.jsonl", "errors")
        >>> print(errors)
        
        >>> # Generate module breakdown
        >>> modules = generate_report(
        ...     "logs/app.log.jsonl",
        ...     "modules",
        ...     output_file="reports/module_breakdown.txt"
        ... )
    """
    reporter = LogReport(log_file_path)
    
    if report_type == "summary":
        report = reporter.summary()
    elif report_type == "full":
        report = reporter.full_report(**kwargs)
    elif report_type == "errors":
        report = reporter.errors_and_criticals()
    elif report_type == "modules":
        report = reporter.module_breakdown()
    else:
        raise ValueError(f"Unknown report type: {report_type}")
    
    if output_file:
        reporter.save_report(report, output_file)
    
    return report
