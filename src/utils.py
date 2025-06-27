"""
FAIR-PRICE Utility Functions

Common utilities and helper functions used across the project.
Includes file operations, logging, progress tracking, and error handling.
"""

import logging
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
from functools import wraps
import os
import pandas as pd

from .config import Config, LoggingConfig, FileConfig

# Type variables for generic functions
T = TypeVar('T')
F = TypeVar('F', bound=Callable[..., Any])


class FileUtils:
    """Utilities for file operations"""
    
    @staticmethod
    def ensure_directory(path: Union[str, Path]) -> Path:
        """
        Ensure directory exists, create if it doesn't
        
        Args:
            path: Directory path to create
            
        Returns:
            Path object for the directory
        """
        path_obj = Path(path)
        path_obj.mkdir(parents=True, exist_ok=True)
        return path_obj
    
    @staticmethod
    def get_file_size_mb(file_path: Union[str, Path]) -> float:
        """
        Get file size in megabytes
        
        Args:
            file_path: Path to the file
            
        Returns:
            File size in MB
        """
        try:
            size_bytes = Path(file_path).stat().st_size
            return size_bytes / (1024 * 1024)
        except FileNotFoundError:
            return 0.0
    
    @staticmethod
    def is_file_valid_size(file_path: Union[str, Path]) -> bool:
        """
        Check if file size is within valid range
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if file size is valid
        """
        size_mb = FileUtils.get_file_size_mb(file_path)
        return FileConfig.MIN_FILE_SIZE_MB <= size_mb <= FileConfig.MAX_FILE_SIZE_MB
    
    @staticmethod
    def list_csv_files(directory: Union[str, Path]) -> List[Path]:
        """
        List all CSV files in a directory
        
        Args:
            directory: Directory to search
            
        Returns:
            List of CSV file paths
        """
        directory_path = Path(directory)
        if not directory_path.exists():
            return []
        
        return list(directory_path.glob("*.csv"))
    
    @staticmethod
    def clean_filename(filename: str) -> str:
        """
        Clean filename by removing invalid characters
        
        Args:
            filename: Original filename
            
        Returns:
            Cleaned filename
        """
        # Remove invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Remove multiple underscores
        while '__' in filename:
            filename = filename.replace('__', '_')
        
        return filename.strip('_')


class ProgressTracker:
    """Simple progress tracking utility"""
    
    def __init__(self, total_items: int, description: str = "Processing"):
        self.total_items = total_items
        self.current_item = 0
        self.description = description
        self.start_time = time.time()
        self.last_update = 0
    
    def update(self, increment: int = 1, status: str = "") -> None:
        """
        Update progress
        
        Args:
            increment: Number of items processed
            status: Optional status message
        """
        self.current_item += increment
        
        # Only update every 0.5 seconds to avoid spam
        current_time = time.time()
        if current_time - self.last_update < 0.5 and self.current_item < self.total_items:
            return
        
        self.last_update = current_time
        
        if self.total_items > 0:
            percentage = (self.current_item / self.total_items) * 100
            elapsed_time = current_time - self.start_time
            
            if self.current_item > 0:
                estimated_total = elapsed_time * (self.total_items / self.current_item)
                remaining_time = estimated_total - elapsed_time
                time_str = f" (ETA: {remaining_time:.1f}s)"
            else:
                time_str = ""
            
            status_msg = f" - {status}" if status else ""
            print(f"   📊 {self.description}: {percentage:.1f}% ({self.current_item}/{self.total_items}){time_str}{status_msg}")
        else:
            print(f"   📊 {self.description}: {self.current_item} items processed")
    
    def finish(self, success_message: str = "Completed") -> None:
        """Mark progress as finished"""
        elapsed_time = time.time() - self.start_time
        print(f"   ✅ {success_message} ({elapsed_time:.1f}s)")


class LoggerSetup:
    """Utility for setting up project logging"""
    
    @staticmethod
    def setup_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
        """
        Set up a logger with consistent formatting
        
        Args:
            name: Logger name
            log_file: Optional log file path
            
        Returns:
            Configured logger
        """
        logger = logging.getLogger(name)
        
        # Prevent duplicate handlers
        if logger.hasHandlers():
            return logger
        
        logger.setLevel(getattr(logging, LoggingConfig.LOG_LEVEL))
        
        # Create formatter
        formatter = logging.Formatter(
            LoggingConfig.LOG_FORMAT,
            datefmt=LoggingConfig.DATE_FORMAT
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler if specified
        if log_file:
            LoggingConfig.create_log_directory()
            file_path = LoggingConfig.LOG_DIR / log_file
            file_handler = logging.FileHandler(file_path)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
    
    @staticmethod
    def get_project_logger() -> logging.Logger:
        """Get the main project logger"""
        return LoggerSetup.setup_logger(
            Config.PROJECT_NAME.lower(),
            LoggingConfig.MAIN_LOG_FILE
        )


def retry_on_exception(
    max_retries: int = 3,
    delay_seconds: float = 1.0,
    exceptions: tuple = (Exception,)
) -> Callable[[F], F]:
    """
    Decorator to retry function calls on exceptions
    
    Args:
        max_retries: Maximum number of retry attempts
        delay_seconds: Delay between retries
        exceptions: Tuple of exceptions to catch
        
    Returns:
        Decorated function
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger = LoggerSetup.get_project_logger()
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay_seconds}s..."
                        )
                        time.sleep(delay_seconds)
                    else:
                        logger = LoggerSetup.get_project_logger()
                        logger.error(f"All retry attempts failed for {func.__name__}")
            
            # Re-raise the last exception if all attempts failed
            raise last_exception
        
        return wrapper
    return decorator


def timing_decorator(func: F) -> F:
    """
    Decorator to measure and log function execution time
    
    Args:
        func: Function to time
        
    Returns:
        Decorated function
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger = LoggerSetup.get_project_logger()
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"{func.__name__} completed in {execution_time:.2f}s")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{func.__name__} failed after {execution_time:.2f}s: {e}")
            raise
    
    return wrapper


class DataFrameUtils:
    """Utilities for DataFrame operations"""
    
    @staticmethod
    def quick_info(df: pd.DataFrame, name: str = "DataFrame") -> Dict[str, Any]:
        """
        Get quick information about a DataFrame
        
        Args:
            df: DataFrame to analyze
            name: Name for the DataFrame
            
        Returns:
            Dictionary with basic info
        """
        return {
            'name': name,
            'shape': df.shape,
            'columns': list(df.columns),
            'dtypes': df.dtypes.to_dict(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
            'missing_values': df.isnull().sum().to_dict(),
            'duplicate_rows': df.duplicated().sum()
        }
    
    @staticmethod
    def safe_read_csv(
        file_path: Union[str, Path],
        encodings: Optional[List[str]] = None,
        separators: Optional[List[str]] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        Safely read CSV with multiple encoding attempts
        
        Args:
            file_path: Path to CSV file
            encodings: List of encodings to try
            separators: List of separators to try
            **kwargs: Additional pandas read_csv arguments
            
        Returns:
            DataFrame if successful, None otherwise
        """
        encodings = encodings or FileConfig.CSV_ENCODINGS
        separators = separators or FileConfig.CSV_SEPARATORS
        
        logger = LoggerSetup.get_project_logger()
        
        for encoding in encodings:
            for separator in separators:
                try:
                    df = pd.read_csv(
                        file_path,
                        encoding=encoding,
                        sep=separator,
                        on_bad_lines='skip',
                        engine='python',
                        **kwargs
                    )
                    
                    # Validate that we got reasonable data
                    if df.shape[1] > 5:  # Should have multiple columns
                        logger.info(f"Successfully read {file_path} with encoding={encoding}, sep='{separator}'")
                        return df
                        
                except Exception as e:
                    continue
        
        logger.error(f"Failed to read {file_path} with any encoding/separator combination")
        return None


class StatusPrinter:
    """Utility for consistent status printing"""
    
    @staticmethod
    def print_header(title: str, width: int = 60) -> None:
        """Print a formatted header"""
        print("=" * width)
        print(f"🎯 {title}")
        print("=" * width)
    
    @staticmethod
    def print_section(title: str, width: int = 50) -> None:
        """Print a formatted section header"""
        print(f"\n📋 {title}")
        print("-" * width)
    
    @staticmethod
    def print_success(message: str) -> None:
        """Print a success message"""
        print(f"✅ {message}")
    
    @staticmethod
    def print_warning(message: str) -> None:
        """Print a warning message"""
        print(f"⚠️  {message}")
    
    @staticmethod
    def print_error(message: str) -> None:
        """Print an error message"""
        print(f"❌ {message}")
    
    @staticmethod
    def print_info(message: str) -> None:
        """Print an info message"""
        print(f"📝 {message}")
    
    @staticmethod
    def print_progress(current: int, total: int, description: str = "") -> None:
        """Print progress information"""
        if total > 0:
            percentage = (current / total) * 100
            desc = f" - {description}" if description else ""
            print(f"📊 Progress: {percentage:.1f}% ({current}/{total}){desc}")


class ValidationUtils:
    """Utilities for data validation"""
    
    @staticmethod
    def validate_brazilian_cnpj(cnpj: str) -> bool:
        """
        Validate Brazilian CNPJ format
        
        Args:
            cnpj: CNPJ string to validate
            
        Returns:
            True if valid format
        """
        if not cnpj or not isinstance(cnpj, str):
            return False
        
        import re
        from .config import DataQualityConfig
        
        # Check basic format
        if re.match(DataQualityConfig.CNPJ_PATTERN, cnpj):
            return True
        
        # Check numeric format (14 digits)
        if re.match(r'^\d{14}$', cnpj):
            return True
        
        return False
    
    @staticmethod
    def is_valid_year(year: Any) -> bool:
        """
        Check if year is valid for this project
        
        Args:
            year: Year value to validate
            
        Returns:
            True if valid year
        """
        try:
            year_int = int(year)
            return 2020 <= year_int <= 2030
        except (ValueError, TypeError):
            return False


# Export main utilities
__all__ = [
    'FileUtils',
    'ProgressTracker', 
    'LoggerSetup',
    'DataFrameUtils',
    'StatusPrinter',
    'ValidationUtils',
    'retry_on_exception',
    'timing_decorator'
]