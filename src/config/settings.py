"""
Configuration settings for Fair-Price Brazilian Health Data Pipeline

Centralizes all configuration including URLs, file paths, processing parameters,
and environment-specific settings for the health economics data extraction.
"""

from pathlib import Path
from typing import Dict, List, Any, Optional
import os
from datetime import datetime


class FairPriceConfig:
    """
    Centralized configuration class for Fair-Price pipeline
    
    Provides configuration for:
    - OpenDataSUS web scraping parameters
    - File paths and directory structure
    - Data processing settings
    - Brazilian-specific validation rules
    """
    
    def __init__(self):
        self.project_root = self._detect_project_root()
        self._setup_directories()
    
    def _detect_project_root(self) -> Path:
        """
        Automatically detect project root from current working directory
        
        Returns:
            Path to project root directory
        """
        current_path = Path.cwd()
        
        # If running from notebooks/ directory, go up one level
        if current_path.name == "notebooks":
            return current_path.parent
        
        # If running from project root
        if (current_path / "src").exists():
            return current_path
        
        # If running from src/ or subdirectories, find project root
        for parent in current_path.parents:
            if (parent / "src").exists() and (parent / "notebooks").exists():
                return parent
        
        # Default to current directory
        return current_path
    
    def _setup_directories(self):
        """Create necessary directories if they don't exist"""
        for directory in [self.raw_data_dir, self.processed_data_dir, 
                         self.output_data_dir, self.reports_dir, self.logs_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    # =============================================================================
    # WEB SCRAPING CONFIGURATION
    # =============================================================================
    
    @property
    def opendatasus_config(self) -> Dict[str, Any]:
        """OpenDataSUS web scraping configuration"""
        return {
            "base_url": "https://opendatasus.saude.gov.br",
            "bps_url": "https://opendatasus.saude.gov.br/dataset/bps",
            "headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            },
            "timeout": 30,
            "max_retries": 3,
            "retry_delay": 2,  # seconds
            "request_delay": 1,  # seconds between requests (be respectful)
        }
    
    @property
    def target_years(self) -> List[int]:
        """Years to extract from OpenDataSUS"""
        return [2020, 2021, 2022, 2023, 2024]
    
    @property
    def csv_patterns(self) -> Dict[str, str]:
        """Patterns for identifying CSV download links"""
        return {
            "csv_extension": ".csv.zip",
            "year_pattern": r"(202[0-4])",
            "s3_base": "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/BPS/csv/",
            "expected_filename_format": "{year}.csv"
        }
    
    # =============================================================================
    # DIRECTORY STRUCTURE
    # =============================================================================
    
    @property
    def data_dir(self) -> Path:
        """Main data directory"""
        return self.project_root / "data"
    
    @property
    def raw_data_dir(self) -> Path:
        """Directory for raw CSV files from OpenDataSUS"""
        return self.data_dir / "raw"
    
    @property
    def processed_data_dir(self) -> Path:
        """Directory for cleaned/standardized data"""
        return self.data_dir / "processed"
    
    @property
    def output_data_dir(self) -> Path:
        """Directory for final consolidated data"""
        return self.data_dir / "output"
    
    @property
    def reports_dir(self) -> Path:
        """Directory for generated reports"""
        return self.project_root / "reports"
    
    @property
    def exploration_reports_dir(self) -> Path:
        """Directory for data exploration reports"""
        return self.reports_dir / "exploration"
    
    @property
    def quality_reports_dir(self) -> Path:
        """Directory for data quality reports"""
        return self.reports_dir / "quality"
    
    @property
    def final_reports_dir(self) -> Path:
        """Directory for final analysis reports"""
        return self.reports_dir / "final"
    
    @property
    def logs_dir(self) -> Path:
        """Directory for log files"""
        return self.project_root / "logs"
    
    # =============================================================================
    # FILE NAMING CONVENTIONS
    # =============================================================================
    
    def get_raw_csv_filename(self, year: int) -> str:
        """Generate filename for raw CSV file"""
        return f"{year}.csv"
    
    def get_processed_csv_filename(self, year: int) -> str:
        """Generate filename for processed CSV file"""
        return f"{year}_processed.csv"
    
    def get_final_csv_filename(self) -> str:
        """Generate filename for final consolidated CSV"""
        timestamp = datetime.now().strftime("%Y%m%d")
        return f"fair_price_consolidated_{timestamp}.csv"
    
    def get_exploration_report_filename(self, year: int) -> str:
        """Generate filename for exploration report"""
        return f"exploration_report_{year}.txt"
    
    def get_quality_report_filename(self) -> str:
        """Generate filename for quality report"""
        timestamp = datetime.now().strftime("%Y%m%d")
        return f"quality_report_{timestamp}.txt"
    
    def get_comparison_report_filename(self) -> str:
        """Generate filename for cross-year comparison report"""
        timestamp = datetime.now().strftime("%Y%m%d")
        return f"comparison_report_{timestamp}.txt"
    
    # =============================================================================
    # DATA PROCESSING CONFIGURATION
    # =============================================================================
    
    @property
    def unified_schema(self) -> List[str]:
        """Unified column schema for standardized data"""
        return [
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
            'preco_total'             # Total price (calculated or provided)
        ]
    
    @property
    def column_mapping(self) -> Dict[str, str]:
        """Map various column names to unified schema"""
        return {
            # Year
            'Ano': 'ano',
            
            # Codes and descriptions
            'Código_BR': 'codigo_br',
            'Descrição_CATMAT': 'descricao_catmat',
            
            # Units and supply
            'Unidade_de_Fornecimento': 'unidade_fornecimento',
            'Unidade_Fornecimento': 'unidade_fornecimento',
            
            # Generic flag
            'Genérico': 'generico',
            
            # Regulatory
            'Anvisa': 'anvisa',
            'ANVISA': 'anvisa',
            
            # Purchase information
            'Compra': 'compra',
            'Modalidade_da_Compra': 'modalidade_compra',
            'Inserção': 'insercao',
            'Tipo_Compra': 'tipo_compra',
            
            # Manufacturer
            'Fabricante': 'fabricante',
            'CNPJ_Fabricante': 'cnpj_fabricante',
            
            # Supplier
            'Fornecedor': 'fornecedor',
            'CNPJ_Fornecedor': 'cnpj_fornecedor',
            
            # Institution
            'Nome_Instituição': 'nome_instituicao',
            'CNPJ_Instituição': 'cnpj_instituicao',
            'Município_Instituição': 'municipio_instituicao',
            
            # Geographic
            'UF': 'uf',
            
            # Quantities and prices
            'Qtd_Itens_Comprados': 'qtd_itens_comprados',
            'Preço_Unitário': 'preco_unitario',
            'Preço_Total': 'preco_total'
        }
    
    @property
    def data_type_mapping(self) -> Dict[str, str]:
        """Target data types for unified schema"""
        return {
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
    
    # =============================================================================
    # BRAZILIAN-SPECIFIC VALIDATION RULES
    # =============================================================================
    
    @property
    def brazilian_states(self) -> List[str]:
        """Valid Brazilian state abbreviations"""
        return [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
            'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
            'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]
    
    @property
    def cnpj_validation(self) -> Dict[str, Any]:
        """CNPJ validation configuration"""
        return {
            "expected_length": 14,
            "formatted_pattern": r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$",
            "numeric_pattern": r"^\d{14}$",
            "format_template": "{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
        }
    
    @property
    def currency_cleaning(self) -> Dict[str, Any]:
        """Brazilian currency cleaning configuration"""
        return {
            "currency_symbol": "R$",
            "decimal_separator": ",",
            "thousands_separator": ".",
            "decimal_places": 2,
            "patterns_to_remove": [r"R\$\s*", r"[^\d,.,-]"],
            "brazilian_format_pattern": r"\d{1,3}(?:\.\d{3})*,\d{2}"
        }
    
    @property
    def encoding_fixes(self) -> Dict[str, str]:
        """Common encoding fixes for Brazilian text"""
        return {
            # Basic accented characters
            'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú',
            'Ã£': 'ã', 'Ãµ': 'õ', 'Ã§': 'ç',
            'Ã¢': 'â', 'Ãª': 'ê', 'Ã´': 'ô',
            
            # Capital letters
            'Ã': 'Á', 'Ã‰': 'É', 'Ã': 'Í', 'Ã"': 'Ó', 'Ãš': 'Ú',
            'Ãƒ': 'Ã', 'Ã•': 'Õ', 'Ã‡': 'Ç',
            'Ã‚': 'Â', 'ÃŠ': 'Ê', 'Ã"': 'Ô',
            
            # Common word patterns
            'institui��o': 'instituição',
            'descri��o': 'descrição',
            'munic�pio': 'município',
            'pre�o': 'preço',
            'gen�rico': 'genérico',
            'inser��o': 'inserção',
            'c�digo': 'código',
            'unit�rio': 'unitário',
            
            # Remove artifacts
            '�': '',
            'Â': '',
        }
    
    # =============================================================================
    # DATA QUALITY THRESHOLDS
    # =============================================================================
    
    @property
    def quality_thresholds(self) -> Dict[str, Any]:
        """Data quality validation thresholds"""
        return {
            "min_rows_per_year": 1000,          # Minimum rows expected per year
            "max_missing_percentage": 50,        # Maximum % of missing values allowed
            "min_data_completeness": 70,         # Minimum % overall data completeness
            "max_duplicate_percentage": 5,       # Maximum % of duplicate rows
            "required_columns": [                # Columns that must be present
                'codigo_br', 'descricao_catmat', 'nome_instituicao'
            ],
            "critical_columns": [                # Columns that cannot be mostly empty
                'codigo_br', 'descricao_catmat'
            ]
        }
    
    # =============================================================================
    # PERFORMANCE CONFIGURATION
    # =============================================================================
    
    @property
    def performance_config(self) -> Dict[str, Any]:
        """Performance and processing configuration"""
        return {
            "chunk_size": 10000,                # Pandas read_csv chunk size
            "max_workers": 4,                   # Maximum threads for parallel processing
            "memory_limit_mb": 1024,            # Memory limit for large operations
            "progress_update_frequency": 1000,   # Update progress every N rows
            "file_size_warning_mb": 100,        # Warn if file exceeds this size
        }
    
    # =============================================================================
    # LOGGING CONFIGURATION
    # =============================================================================
    
    @property
    def logging_config(self) -> Dict[str, Any]:
        """Logging configuration"""
        return {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "date_format": "%Y-%m-%d %H:%M:%S",
            "log_to_file": True,
            "log_to_console": True,
            "max_log_size_mb": 10,
            "backup_count": 3
        }
    
    # =============================================================================
    # ENVIRONMENT-SPECIFIC SETTINGS
    # =============================================================================
    
    @property
    def is_development(self) -> bool:
        """Check if running in development mode"""
        return os.getenv("FAIR_PRICE_ENV", "development") == "development"
    
    @property
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return os.getenv("FAIR_PRICE_ENV", "development") == "production"
    
    def get_environment_config(self) -> Dict[str, Any]:
        """Get environment-specific configuration"""
        if self.is_production:
            return {
                "log_level": "WARNING",
                "enable_debug": False,
                "strict_validation": True,
                "performance_monitoring": True
            }
        else:
            return {
                "log_level": "INFO",
                "enable_debug": True,
                "strict_validation": False,
                "performance_monitoring": False
            }
    
    # =============================================================================
    # UTILITY METHODS
    # =============================================================================
    
    def get_full_file_path(self, directory: str, filename: str) -> Path:
        """
        Get full file path for a given directory and filename
        
        Args:
            directory: Directory type ('raw', 'processed', 'output', 'reports')
            filename: Name of the file
            
        Returns:
            Full Path object
        """
        directory_map = {
            'raw': self.raw_data_dir,
            'processed': self.processed_data_dir,
            'output': self.output_data_dir,
            'reports': self.reports_dir,
            'exploration': self.exploration_reports_dir,
            'quality': self.quality_reports_dir,
            'final': self.final_reports_dir,
            'logs': self.logs_dir
        }
        
        if directory not in directory_map:
            raise ValueError(f"Unknown directory type: {directory}")
        
        return directory_map[directory] / filename
    
    def validate_configuration(self) -> Dict[str, bool]:
        """
        Validate configuration and environment setup
        
        Returns:
            Dictionary with validation results
        """
        validation_results = {}
        
        # Check directories exist
        validation_results['directories_exist'] = all([
            self.raw_data_dir.exists(),
            self.processed_data_dir.exists(),
            self.output_data_dir.exists(),
            self.reports_dir.exists(),
            self.logs_dir.exists()
        ])
        
        # Check write permissions
        try:
            test_file = self.logs_dir / "test_write.tmp"
            test_file.write_text("test")
            test_file.unlink()
            validation_results['write_permissions'] = True
        except Exception:
            validation_results['write_permissions'] = False
        
        # Check required data types are valid
        validation_results['schema_valid'] = len(self.unified_schema) == len(set(self.unified_schema))
        
        # Check years are reasonable
        current_year = datetime.now().year
        validation_results['years_valid'] = all(
            2020 <= year <= current_year for year in self.target_years
        )
        
        return validation_results
    
    def print_configuration_summary(self):
        """Print a summary of current configuration"""
        print("=" * 60)
        print("FAIR-PRICE CONFIGURATION SUMMARY")
        print("=" * 60)
        print(f"Project Root: {self.project_root}")
        print(f"Target Years: {self.target_years}")
        print(f"Data Directory: {self.data_dir}")
        print(f"Reports Directory: {self.reports_dir}")
        print(f"Environment: {'Production' if self.is_production else 'Development'}")
        print(f"Schema Columns: {len(self.unified_schema)}")
        print(f"Column Mappings: {len(self.column_mapping)}")
        print("=" * 60)
        
        # Validation results
        validation = self.validate_configuration()
        print("VALIDATION RESULTS:")
        for key, result in validation.items():
            status = "✅" if result else "❌"
            print(f"  {status} {key}: {result}")
        print("=" * 60)


# Create global configuration instance
config = FairPriceConfig()


# Convenience functions for easy access
def get_config() -> FairPriceConfig:
    """Get the global configuration instance"""
    return config


def get_raw_data_path(year: int) -> Path:
    """Get path for raw data file"""
    filename = config.get_raw_csv_filename(year)
    return config.get_full_file_path('raw', filename)


def get_processed_data_path(year: int) -> Path:
    """Get path for processed data file"""
    filename = config.get_processed_csv_filename(year)
    return config.get_full_file_path('processed', filename)


def get_output_data_path() -> Path:
    """Get path for final output file"""
    filename = config.get_final_csv_filename()
    return config.get_full_file_path('output', filename)


# Example usage and testing
if __name__ == "__main__":
    # Test configuration
    config.print_configuration_summary()
    
    # Test some paths
    print("\nEXAMPLE PATHS:")
    print(f"Raw 2024 file: {get_raw_data_path(2024)}")
    print(f"Processed 2023 file: {get_processed_data_path(2023)}")
    print(f"Final output: {get_output_data_path()}")
    
    # Test validation
    validation = config.validate_configuration()
    all_valid = all(validation.values())
    print(f"\n{'✅' if all_valid else '❌'} Configuration valid: {all_valid}")