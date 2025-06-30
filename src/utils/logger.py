"""
Logging configuration for Fair-Price Brazilian Health Data Pipeline

Provides centralized logging setup with professional formatting,
file output, and different levels for development and production use.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


def setup_logger(
    name: str, 
    log_file: Optional[str] = None, 
    level: int = logging.INFO,
    console_output: bool = True
) -> logging.Logger:
    """
    Set up a logger with professional formatting and optional file output
    
    Args:
        name: Logger name (typically __name__ from calling module)
        log_file: Optional file path for log output
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        console_output: Whether to output to console
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Prevent duplicate handlers if logger already exists
    if logger.handlers:
        return logger
    
    # Create formatter with timestamp and module info
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console handler for immediate feedback
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler for persistent logging
    if log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_default_log_file(module_name: str) -> str:
    """
    Generate default log file path based on module name and timestamp
    
    Args:
        module_name: Name of the calling module
        
    Returns:
        Log file path string
    """
    timestamp = datetime.now().strftime("%Y%m%d")
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    return str(log_dir / f"{module_name}_{timestamp}.log")


def log_execution_time(logger: logging.Logger):
    """
    Decorator to log execution time of functions
    
    Args:
        logger: Logger instance to use for timing logs
        
    Usage:
        @log_execution_time(logger)
        def my_function():
            pass
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            logger.info(f"Starting {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                logger.info(f"Completed {func.__name__} in {duration:.2f} seconds")
                return result
                
            except Exception as e:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                logger.error(f"Failed {func.__name__} after {duration:.2f} seconds: {str(e)}")
                raise
                
        return wrapper
    return decorator


def log_data_operation(logger: logging.Logger, operation: str):
    """
    Decorator to log data operations with input/output information
    
    Args:
        logger: Logger instance to use
        operation: Description of the operation being performed
        
    Usage:
        @log_data_operation(logger, "CSV extraction")
        def extract_csv(year):
            return data
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.info(f"🚀 Starting {operation}")
            start_time = datetime.now()
            
            try:
                result = func(*args, **kwargs)
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                # Log result information if it's a data structure
                if hasattr(result, '__len__'):
                    try:
                        if hasattr(result, 'shape'):  # DataFrame
                            logger.info(f"✅ {operation} completed: {result.shape[0]:,} rows × {result.shape[1]} columns ({duration:.2f}s)")
                        elif isinstance(result, dict):  # Dictionary result
                            logger.info(f"✅ {operation} completed: {len(result)} items ({duration:.2f}s)")
                        elif isinstance(result, (list, tuple)):  # List/tuple result
                            logger.info(f"✅ {operation} completed: {len(result)} items ({duration:.2f}s)")
                        else:
                            logger.info(f"✅ {operation} completed ({duration:.2f}s)")
                    except:
                        logger.info(f"✅ {operation} completed ({duration:.2f}s)")
                else:
                    logger.info(f"✅ {operation} completed ({duration:.2f}s)")
                
                return result
                
            except Exception as e:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                logger.error(f"❌ {operation} failed after {duration:.2f}s: {str(e)}")
                raise
                
        return wrapper
    return decorator


def log_progress(logger: logging.Logger, total: int, description: str = "Processing"):
    """
    Context manager for logging progress of iterative operations
    
    Args:
        logger: Logger instance
        total: Total number of items to process
        description: Description of what's being processed
        
    Usage:
        with log_progress(logger, len(files), "Processing CSV files") as progress:
            for i, file in enumerate(files):
                # do work
                progress(i + 1)
    """
    class ProgressLogger:
        def __init__(self, logger, total, description):
            self.logger = logger
            self.total = total
            self.description = description
            self.start_time = datetime.now()
            
        def __enter__(self):
            self.logger.info(f"📊 {self.description}: 0/{self.total} items")
            return self
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is None:
                duration = (datetime.now() - self.start_time).total_seconds()
                self.logger.info(f"🎯 {self.description} completed: {self.total}/{self.total} items ({duration:.2f}s)")
            
        def __call__(self, current: int):
            if current % max(1, self.total // 10) == 0 or current == self.total:
                percent = (current / self.total) * 100
                elapsed = (datetime.now() - self.start_time).total_seconds()
                self.logger.info(f"📈 {self.description}: {current}/{self.total} items ({percent:.1f}%) - {elapsed:.1f}s elapsed")
    
    return ProgressLogger(logger, total, description)


# Module-level convenience functions
def get_extraction_logger() -> logging.Logger:
    """Get logger for extraction module"""
    return setup_logger(
        "fair_price.extraction",
        log_file=get_default_log_file("extraction"),
        level=logging.INFO
    )


def get_standardization_logger() -> logging.Logger:
    """Get logger for standardization module"""
    return setup_logger(
        "fair_price.standardization", 
        log_file=get_default_log_file("standardization"),
        level=logging.INFO
    )


def get_exploration_logger() -> logging.Logger:
    """Get logger for exploration module"""
    return setup_logger(
        "fair_price.exploration",
        log_file=get_default_log_file("exploration"),
        level=logging.INFO
    )


def get_consolidation_logger() -> logging.Logger:
    """Get logger for consolidation module"""
    return setup_logger(
        "fair_price.consolidation",
        log_file=get_default_log_file("consolidation"), 
        level=logging.INFO
    )


# Example usage and testing
if __name__ == "__main__":
    # Test the logging setup
    test_logger = setup_logger("test_module", log_file="logs/test.log")
    
    test_logger.info("Logger setup test - INFO level")
    test_logger.warning("Logger setup test - WARNING level")
    test_logger.error("Logger setup test - ERROR level")
    
    # Test decorators
    @log_execution_time(test_logger)
    @log_data_operation(test_logger, "Test data processing")
    def test_function():
        import time
        time.sleep(1)
        return {"test": "data", "rows": 1000}
    
    result = test_function()
    
    # Test progress logging
    with log_progress(test_logger, 5, "Test processing") as progress:
        import time
        for i in range(5):
            time.sleep(0.2)
            progress(i + 1)
    
    print("✅ Logger module test completed - check logs/ directory for output files")