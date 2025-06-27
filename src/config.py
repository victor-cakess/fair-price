"""
FAIR-PRICE Configuration Module

Centralized configuration for the Brazilian Health Economics Data Pipeline.
Contains all settings, paths, and constants used across the project.
"""

from pathlib import Path
from typing import Dict, List, Optional
import os


class Config:
    """Main configuration class for FAIR-PRICE project"""
    
    # Project Information
    PROJECT_NAME: str = "FAIR-PRICE"
    PROJECT_DESCRIPTION: str = "Brazilian Health Economics Data Pipeline"
    VERSION: str = "1.0.0"
    
    # Base Paths
    PROJECT_ROOT: Path = Path(__file__).parent.parent
    SRC_DIR: Path = PROJECT_ROOT / "src"
    DATA_DIR: Path = PROJECT_ROOT / "data"
    REPORTS_DIR: Path = PROJECT_ROOT / "reports"
    TESTS_DIR: Path = PROJECT_ROOT / "tests"
    
    # Data Storage Paths
    RAW_DATA_DIR: Path = DATA_DIR / "raw"
    PROCESSED_DATA_DIR: Path = DATA_DIR / "processed"
    
    # Ensure directories exist
    @classmethod
    def create_directories(cls) -> None:
        """Create necessary directories if they don't exist"""
        directories = [
            cls.DATA_DIR,
            cls.RAW_DATA_DIR,
            cls.PROCESSED_DATA_DIR,
            cls.REPORTS_DIR
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)


class WebScrapingConfig:
    """Configuration for web scraping operations"""
    
    # Brazilian Health Data URLs
    BASE_URL: str = "https://opendatasus.saude.gov.br"
    BPS_URL: str = "https://opendatasus.saude.gov.br/dataset/bps"
    S3_BASE_URL: str = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/BPS/csv"
    
    # HTTP Configuration
    REQUEST_HEADERS: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    # Request Settings
    TIMEOUT_SECONDS: int = 30
    MAX_RETRIES: int = 3
    RETRY_DELAY_SECONDS: int = 2
    DOWNLOAD_CHUNK_SIZE: int = 8192
    PAUSE_BETWEEN_DOWNLOADS: int = 3  # Be respectful to the server
    
    # Years to process
    AVAILABLE_YEARS: List[int] = [2020, 2021, 2022, 2023, 2024]
    DEFAULT_YEARS: List[int] = [2024]  # For testing


class FileConfig:
    """Configuration for file operations"""
    
    # File Validation
    MIN_FILE_SIZE_MB: float = 0.1
    MAX_FILE_SIZE_MB: float = 100.0
    
    # CSV Settings
    CSV_ENCODINGS: List[str] = ['utf-8', 'latin-1', 'iso-8859-1', 'windows-1252']
    CSV_SEPARATORS: List[str] = [',', ';', '\t']
    
    # File Extensions
    ALLOWED_EXTENSIONS: List[str] = ['.csv', '.zip']
    
    # Output Files
    STANDARDIZED_OUTPUT: str = "standardized_data.csv"
    EXPLORATION_REPORT_PREFIX: str = "exploration_report"
    COMPARISON_REPORT: str = "comparison_report.txt"


class DataQualityConfig:
    """Configuration for data quality and validation"""
    
    # Quality Thresholds
    MIN_QUALITY_SCORE: float = 70.0
    HIGH_QUALITY_THRESHOLD: float = 90.0
    
    # Validation Rules
    REQUIRED_COLUMNS: List[str] = [
        'ano', 'codigo_br', 'descricao_catmat'
    ]
    
    # Brazilian Identifiers
    CNPJ_PATTERN: str = r'^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$'
    CNES_PATTERN: str = r'^\d{7}$'
    IBGE_CODE_PATTERN: str = r'^\d{6,7}$'
    
    # Currency Settings
    CURRENCY_SYMBOL: str = 'R$'
    DECIMAL_SEPARATOR: str = ','
    THOUSANDS_SEPARATOR: str = '.'


class LoggingConfig:
    """Configuration for logging"""
    
    # Log Levels
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    # Log Format
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
    
    # Log Files
    LOG_DIR: Path = Config.PROJECT_ROOT / "logs"
    MAIN_LOG_FILE: str = "fair_price.log"
    ERROR_LOG_FILE: str = "errors.log"
    
    # Ensure log directory exists
    @classmethod
    def create_log_directory(cls) -> None:
        """Create log directory if it doesn't exist"""
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)


class UnifiedSchemaConfig:
    """Configuration for the unified data schema"""
    
    # Standard column names for all years
    UNIFIED_COLUMNS: List[str] = [
        'ano',                    # Year of the transaction
        'codigo_br',              # BR code identifier
        'descricao_catmat',       # CATMAT description
        'unidade_fornecimento',   # Supply unit
        'generico',               # Generic flag (boolean)
        'anvisa',                 # ANVISA code
        'compra',                 # Purchase date/ID
        'modalidade_compra',      # Purchase modality
        'insercao',               # Insertion date
        'tipo_compra',            # Purchase type
        'fabricante',             # Manufacturer name
        'cnpj_fabricante',        # Manufacturer CNPJ
        'fornecedor',             # Supplier name
        'cnpj_fornecedor',        # Supplier CNPJ
        'nome_instituicao',       # Institution name
        'cnpj_instituicao',       # Institution CNPJ
        'municipio_instituicao',  # Institution municipality
        'uf',                     # State (UF)
        'qtd_itens_comprados',    # Quantity of items purchased
        'preco_unitario',         # Unit price
        'preco_total'             # Total price
    ]
    
    # Data types for unified schema
    COLUMN_TYPES: Dict[str, str] = {
        'ano': 'int16',
        'codigo_br': 'string',
        'descricao_catmat': 'string',
        'unidade_fornecimento': 'string',
        'generico': 'boolean',
        'anvisa': 'string',
        'compra': 'string',
        'modalidade_compra': 'string',
        'insercao': 'string',
        'tipo_compra': 'string',
        'fabricante': 'string',
        'cnpj_fabricante': 'string',
        'fornecedor': 'string',
        'cnpj_fornecedor': 'string',
        'nome_instituicao': 'string',
        'cnpj_instituicao': 'string',
        'municipio_instituicao': 'string',
        'uf': 'string',
        'qtd_itens_comprados': 'int32',
        'preco_unitario': 'float64',
        'preco_total': 'float64'
    }


# Environment-specific settings
class EnvironmentConfig:
    """Environment-specific configuration"""
    
    # Current environment
    ENVIRONMENT: str = os.getenv('FAIR_PRICE_ENV', 'development')
    
    # Debug mode
    DEBUG: bool = os.getenv('FAIR_PRICE_DEBUG', 'True').lower() == 'true'
    
    # Testing mode
    TESTING: bool = ENVIRONMENT == 'testing'
    
    # Production mode
    PRODUCTION: bool = ENVIRONMENT == 'production'


# Convenience function to initialize all configurations
def initialize_project() -> None:
    """Initialize the project by creating necessary directories"""
    Config.create_directories()
    LoggingConfig.create_log_directory()
    
    print(f"✅ {Config.PROJECT_NAME} v{Config.VERSION} initialized")
    print(f"📁 Project root: {Config.PROJECT_ROOT}")
    print(f"📊 Data directory: {Config.DATA_DIR}")
    print(f"📋 Reports directory: {Config.REPORTS_DIR}")


# Export main configuration classes
__all__ = [
    'Config',
    'WebScrapingConfig', 
    'FileConfig',
    'DataQualityConfig',
    'LoggingConfig',
    'UnifiedSchemaConfig',
    'EnvironmentConfig',
    'initialize_project'
]