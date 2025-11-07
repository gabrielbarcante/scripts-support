"""
Log report generator for JSONL log files using pandas DataFrames.

This module provides utilities to parse and generate structured reports
from JSON Lines (.jsonl) log files with DataFrame outputs and Excel export.
"""

import json
from pathlib import Path
from typing import List, Literal
import pandas as pd


class LogReportDataFrame:
    """
    Parse and generate DataFrame-based reports from JSONL log files.
    
    Provides methods to create various reports including summaries,
    filtered views, statistics, and Excel exports.
    """

    def __init__(self, log_file_path: str | Path, fields: List[str] | None = None, column_timestamp: str = "timestamp", format_timestamp: str = "ISO8601") -> None:
        """
        Initialize the log reporter.
        
        Args:
            log_file_path: Path to the .jsonl log file.
            fields: Optional list of fields to include. If None, includes all fields.
            column_timestamp: Name of the timestamp column (default: "timestamp").
            format_timestamp: Format of timestamp. Use "ISO8601" for ISO format or strftime format string.
            
        Raises:
            FileNotFoundError: If the log file doesn't exist.
            ValueError: If the file is not a .jsonl file.
        """
        self.log_file_path = Path(log_file_path)
        
        if not self.log_file_path.exists():
            raise FileNotFoundError(f"Log file not found: {self.log_file_path}")
        
        if self.log_file_path.suffix != ".jsonl":
            raise ValueError(f"Expected .jsonl file, got: {self.log_file_path.suffix}")
        
        self.fields = fields
        self.column_timestamp = column_timestamp
        self.format_timestamp = format_timestamp
        self.df_log: pd.DataFrame = pd.DataFrame()
        self._load_logs()
    
    def _load_logs(self) -> None:
        """Load and parse log entries from the JSONL file into a pandas DataFrame."""
        log_entries = []
        with open(self.log_file_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    log_entries.append(entry)
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping invalid JSON on line {line_num}: {e}")

        # Create DataFrame with specified or all columns
        if self.fields:
            self.df_log = pd.DataFrame(log_entries, columns=self.fields)
        else:
            self.df_log = pd.DataFrame(log_entries)

        # Parse timestamp column if it exists
        if self.column_timestamp in self.df_log.columns:
            self.df_log[self.column_timestamp] = pd.to_datetime(self.df_log[self.column_timestamp], format="ISO8601",errors="coerce")

    
    def summary(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate a summary report of the log file.
        
        Returns:
            A tuple of (summary_df, level_distribution_df).
        """
        if self.df_log.empty:
            empty_df = pd.DataFrame({"Message": ["No log entries found."]})
            return empty_df, pd.DataFrame()
        
        # Basic statistics
        total_entries = len(self.df_log)
        
        # Level distribution
        level_counts = self.df_log["level"].value_counts() if "level" in self.df_log.columns else pd.Series()
        level_percentages = (level_counts / total_entries * 100).round(1)
        
        # Time range
        if self.column_timestamp in self.df_log.columns:
            first_log = self.df_log[self.column_timestamp].min()
            last_log = self.df_log[self.column_timestamp].max()
        else:
            first_log = last_log = "N/A"
        
        # Unique loggers
        unique_loggers = self.df_log["logger"].nunique() if "logger" in self.df_log.columns else 0
        
        # Create summary DataFrame
        summary_data = {
            "Metric": ["Log File", "Total Entries", "First Log", "Last Log", "Unique Loggers"],
            "Value": [
                self.log_file_path.name,
                total_entries,
                str(first_log),
                str(last_log),
                unique_loggers
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        
        # Level distribution DataFrame
        if not level_counts.empty:
            level_data = {
                "Level": level_counts.index,
                "Count": level_counts.values,
                "Percentage": level_percentages.values
            }
            level_df = pd.DataFrame(level_data)
        else:
            level_df = pd.DataFrame({"Level": [], "Count": [], "Percentage": []})
        
        return summary_df, level_df

    
    def filter_by_level(self, level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], limit: int | None = None) -> pd.DataFrame:
        """
        Generate a report filtered by log level.
        
        Args:
            level: The log level to filter by.
            limit: Maximum number of entries to include (most recent first).
        
        Returns:
            A DataFrame with filtered log entries.
        """
        if "level" not in self.df_log.columns:
            return pd.DataFrame({"Message": [f"No 'level' column found in logs."]})
        
        filtered = self.df_log[self.df_log["level"] == level].copy()
        
        if filtered.empty:
            return pd.DataFrame({"Message": [f"No {level} level entries found."]})
        
        if limit:
            filtered = filtered.tail(limit)
        
        return filtered

    
    def filter_by_time_range(self, start_time: str | None = None, end_time: str | None = None) -> pd.DataFrame:
        """
        Generate a report for logs within a time range.
        
        Args:
            start_time: ISO format timestamp (inclusive). If None, from beginning.
            end_time: ISO format timestamp (inclusive). If None, to end.
        
        Returns:
            A DataFrame with filtered log entries.
        """
        if self.column_timestamp not in self.df_log.columns:
            return pd.DataFrame({"Message": [f"No '{self.column_timestamp}' column found in logs."]})
        
        filtered = self.df_log.copy()
        
        if start_time:
            start_dt = pd.to_datetime(start_time)
            filtered = filtered[filtered[self.column_timestamp] >= start_dt]
        
        if end_time:
            end_dt = pd.to_datetime(end_time)
            filtered = filtered[filtered[self.column_timestamp] <= end_dt]
        
        if filtered.empty:
            return pd.DataFrame({"Message": ["No entries found in specified time range."]})
        
        return filtered

    
    def errors_and_criticals(self) -> pd.DataFrame:
        """
        Generate a report of all ERROR and CRITICAL level logs.
        
        Returns:
            A DataFrame with error and critical entries.
        """
        if "level" not in self.df_log.columns:
            return pd.DataFrame({"Message": ["No 'level' column found in logs."]})
        
        filtered = self.df_log[self.df_log["level"].isin(["ERROR", "CRITICAL"])].copy()
        
        if filtered.empty:
            return pd.DataFrame({"Message": ["No errors or critical issues found. ✓"]})
        
        return filtered

    
    def module_breakdown(self) -> pd.DataFrame:
        """
        Generate a report breaking down logs by module.
        
        Returns:
            A DataFrame with per-module statistics.
        """
        if "module" not in self.df_log.columns or "level" not in self.df_log.columns:
            return pd.DataFrame({"Message": ["Required columns 'module' and 'level' not found in logs."]})
        
        # Create pivot table with module as rows and levels as columns
        breakdown = pd.crosstab(
            self.df_log["module"], 
            self.df_log["level"], 
            margins=True, 
            margins_name="Total"
        )
        
        return breakdown

    
    def full_report(self, limit: int | None = None) -> pd.DataFrame:
        """
        Generate a complete detailed report of all logs.
        
        Args:
            limit: Maximum number of entries to include (most recent first).
        
        Returns:
            A DataFrame with all log entries.
        """
        if self.df_log.empty:
            return pd.DataFrame({"Message": ["No log entries found."]})
        
        result = self.df_log.copy()
        
        if limit:
            result = result.tail(limit)
        
        return result
    
    
    def save_to_excel(self, output_path: str | Path, include_summary: bool = True, include_errors: bool = True, include_module_breakdown: bool = True) -> None:
        """
        Save comprehensive log reports to an Excel file with multiple sheets.
        
        Args:
            output_path: Path where the Excel file should be saved.
            include_summary: Whether to include summary sheet.
            include_errors: Whether to include errors and criticals sheet.
            include_module_breakdown: Whether to include module breakdown sheet.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create a copy of the dataframe and convert timezone-aware timestamps to timezone-naive
        df_to_export = self.df_log.copy()
        if self.column_timestamp in df_to_export.columns:
            if pd.api.types.is_datetime64_any_dtype(df_to_export[self.column_timestamp]):
                # Remove timezone information for Excel compatibility
                df_to_export[self.column_timestamp] = df_to_export[self.column_timestamp].dt.tz_localize(None)
        
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # Full log data
            df_to_export.to_excel(writer, sheet_name="Full Logs", index=False)
            
            # Summary
            if include_summary:
                summary_df, level_df = self.summary()
                summary_df.to_excel(writer, sheet_name="Summary", index=False)
                if not level_df.empty:
                    level_df.to_excel(writer, sheet_name="Summary", startrow=len(summary_df) + 2, index=False)
            
            # Errors and Criticals
            if include_errors:
                errors_df = self.errors_and_criticals()
                # Remove timezone from timestamp column if present
                if self.column_timestamp in errors_df.columns:
                    if pd.api.types.is_datetime64_any_dtype(errors_df[self.column_timestamp]):
                        errors_df = errors_df.copy()
                        errors_df[self.column_timestamp] = errors_df[self.column_timestamp].dt.tz_localize(None)
                errors_df.to_excel(writer, sheet_name="Errors & Criticals", index=False)
            
            # Module Breakdown
            if include_module_breakdown:
                module_df = self.module_breakdown()
                module_df.to_excel(writer, sheet_name="Module Breakdown", index=True)
            
            # Level-specific sheets
            levels: List[Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]] = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
            for level in levels:
                level_df = self.filter_by_level(level)
                if not level_df.empty and "Message" not in level_df.columns:
                    # Remove timezone from timestamp column if present
                    if self.column_timestamp in level_df.columns:
                        if pd.api.types.is_datetime64_any_dtype(level_df[self.column_timestamp]):
                            level_df = level_df.copy()
                            level_df[self.column_timestamp] = level_df[self.column_timestamp].dt.tz_localize(None)
                    level_df.to_excel(writer, sheet_name=f"{level} Logs", index=False)
        
        print(f"Excel report saved to: {output_path}")


def generate_df_report(log_file_path: str | Path, report_type: Literal["summary", "full", "errors", "modules", "excel"] = "summary", output_file: str | Path | None = None, **kwargs) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convenience function to generate a report from a log file.
    
    Args:
        log_file_path: Path to the .jsonl log file.
        report_type: Type of report to generate.
            - "summary": Summary statistics and level distribution
            - "full": Complete log data
            - "errors": Errors and critical logs only
            - "modules": Module breakdown statistics
            - "excel": Generate complete Excel file with all sheets
        output_file: If provided and report_type is "excel", save to this Excel file.
        **kwargs: Additional arguments passed to the report method.
    
    Returns:
        The generated report as a DataFrame or tuple of DataFrames.
    
    Example:
        >>> # Generate and display a summary
        >>> summary_df, levels_df = generate_df_report("logs/app.log.jsonl", "summary")
        >>> print(summary_df)
        >>> 
        >>> # Generate complete Excel report
        >>> generate_df_report(
        ...     "logs/app.log.jsonl", 
        ...     "excel", 
        ...     output_file="reports/log_analysis.xlsx"
        ... )
    """
    reporter = LogReportDataFrame(log_file_path)
    
    if report_type == "summary":
        return reporter.summary()
    elif report_type == "full":
        return reporter.full_report(**kwargs)
    elif report_type == "errors":
        return reporter.errors_and_criticals()
    elif report_type == "modules":
        return reporter.module_breakdown()
    elif report_type == "excel":
        if not output_file:
            raise ValueError("output_file is required when report_type is 'excel'")
        reporter.save_to_excel(output_file, **kwargs)
        return pd.DataFrame({"Message": [f"Excel report saved to {output_file}"]})
    else:
        raise ValueError(f"Unknown report type: {report_type}")
