"""
Data exploration and analysis module for Fair-Price Brazilian Health Data Pipeline

Comprehensive data exploration framework that analyzes CSV structure, data quality,
content patterns, and Brazilian-specific patterns. Converts your excellent exploration
work into reusable, professional modules.

Based on your comprehensive exploration notebook with Brazilian health data specifics.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
import re
from collections import Counter
from pathlib import Path

# Import our configuration and logging
from config.settings import get_config
from utils.logger import get_exploration_logger, log_data_operation


class BrazilianHealthDataAnalyzer:
    """
    Comprehensive analyzer for Brazilian Health Economics CSV data
    
    Provides multiple analysis layers:
    - Schema analysis (structure, columns, data types)
    - Data quality assessment (missing values, duplicates, encoding issues)
    - Content pattern analysis (value distributions, categorical candidates)
    - Brazilian-specific analysis (CNPJ, CNES, geographic, currency patterns)
    """
    
    def __init__(self):
        self.config = get_config()
        self.logger = get_exploration_logger()
        self.logger.info("🔍 Brazilian Health Data Analyzer initialized")
    
    @log_data_operation(get_exploration_logger(), "Schema analysis")
    def analyze_schema(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """
        Comprehensive schema analysis of a DataFrame
        
        Args:
            df: DataFrame to analyze
            filename: Name of the file for reference
            
        Returns:
            Dictionary with schema information
        """
        schema_info = {
            'filename': filename,
            'shape': df.shape,
            'columns': list(df.columns),
            'column_count': len(df.columns),
            'row_count': len(df),
            'dtypes': df.dtypes.to_dict(),
            'memory_usage': df.memory_usage(deep=True).sum(),
            'duplicated_columns': self._find_duplicate_columns(df),
            'column_name_patterns': self._analyze_column_patterns(df.columns)
        }
        
        self.logger.info(f"   📊 Schema: {schema_info['shape'][0]:,} rows × {schema_info['shape'][1]} columns")
        self.logger.info(f"   💾 Memory: {schema_info['memory_usage'] / 1024 / 1024:.2f} MB")
        
        return schema_info
    
    @log_data_operation(get_exploration_logger(), "Data quality assessment") 
    def analyze_data_quality(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """
        Comprehensive data quality analysis
        
        Args:
            df: DataFrame to analyze
            filename: Name of the file for reference
            
        Returns:
            Dictionary with data quality metrics
        """
        quality_info = {
            'filename': filename,
            'missing_values': df.isnull().sum().to_dict(),
            'missing_percentage': (df.isnull().sum() / len(df) * 100).to_dict(),
            'duplicate_rows': df.duplicated().sum(),
            'duplicate_percentage': df.duplicated().sum() / len(df) * 100,
            'empty_strings': self._count_empty_strings(df),
            'whitespace_issues': self._detect_whitespace_issues(df),
            'encoding_issues': self._detect_encoding_issues(df),
            'numeric_columns_with_text': self._find_numeric_with_text(df)
        }
        
        # Log key quality metrics
        completeness = (1 - df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
        self.logger.info(f"   🎯 Data completeness: {completeness:.1f}%")
        self.logger.info(f"   🔍 Duplicate rows: {quality_info['duplicate_rows']:,} ({quality_info['duplicate_percentage']:.2f}%)")
        
        return quality_info
    
    @log_data_operation(get_exploration_logger(), "Content pattern analysis")
    def analyze_content_patterns(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """
        Analyze content patterns and value distributions
        
        Args:
            df: DataFrame to analyze
            filename: Name of the file for reference
            
        Returns:
            Dictionary with content analysis
        """
        content_info = {
            'filename': filename,
            'unique_value_counts': df.nunique().to_dict(),
            'brazilian_identifiers': self._detect_brazilian_identifiers(df),
            'geographic_columns': self._detect_geographic_columns(df),
            'financial_columns': self._detect_financial_columns(df),
            'date_columns': self._detect_date_columns(df),
            'categorical_candidates': self._identify_categorical_columns(df),
            'value_length_stats': self._analyze_value_lengths(df)
        }
        
        # Log key patterns
        boolean_cols = len(content_info['categorical_candidates']['boolean_like'])
        geo_cols = len(content_info['geographic_columns'])
        financial_cols = len(content_info['financial_columns'])
        
        self.logger.info(f"   🎭 Boolean-like columns: {boolean_cols}")
        self.logger.info(f"   🌎 Geographic columns: {geo_cols}")
        self.logger.info(f"   💰 Financial columns: {financial_cols}")
        
        return content_info
    
    @log_data_operation(get_exploration_logger(), "Brazilian-specific pattern analysis")
    def analyze_brazilian_specifics(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """
        Analyze Brazilian-specific data patterns
        
        Args:
            df: DataFrame to analyze
            filename: Name of the file for reference
            
        Returns:
            Dictionary with Brazilian-specific analysis
        """
        brazilian_info = {
            'filename': filename,
            'cnpj_patterns': self._analyze_cnpj_patterns(df),
            'cnes_patterns': self._analyze_cnes_patterns(df),
            'ibge_codes': self._analyze_ibge_codes(df),
            'brazilian_currency': self._analyze_currency_patterns(df),
            'coordinate_patterns': self._analyze_coordinate_patterns(df),
            'state_references': self._detect_state_references(df),
            'municipality_patterns': self._detect_municipality_patterns(df)
        }
        
        # Log Brazilian-specific findings
        cnpj_cols = len(brazilian_info['cnpj_patterns'])
        cnes_cols = len(brazilian_info['cnes_patterns'])
        state_cols = len(brazilian_info['state_references'])
        
        self.logger.info(f"   🇧🇷 CNPJ columns: {cnpj_cols}")
        self.logger.info(f"   🏥 CNES columns: {cnes_cols}")
        self.logger.info(f"   🗺️  State columns: {state_cols}")
        
        return brazilian_info
    
    def generate_comprehensive_analysis(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """
        Generate comprehensive exploration analysis combining all analyses
        
        Args:
            df: DataFrame to analyze
            filename: Name of the file for reference
            
        Returns:
            Complete exploration summary
        """
        self.logger.info(f"🔬 Starting comprehensive analysis of {filename}")
        
        analysis = {
            'schema': self.analyze_schema(df, filename),
            'quality': self.analyze_data_quality(df, filename),
            'content': self.analyze_content_patterns(df, filename),
            'brazilian_specifics': self.analyze_brazilian_specifics(df, filename),
            'sample_data': self._get_sample_data(df, filename)
        }
        
        self.logger.info(f"✅ Comprehensive analysis completed for {filename}")
        return analysis
    
    # =============================================================================
    # HELPER METHODS - Schema Analysis
    # =============================================================================
    
    def _find_duplicate_columns(self, df: pd.DataFrame) -> List[str]:
        """Find columns with identical names"""
        return [col for col, count in Counter(df.columns).items() if count > 1]
    
    def _analyze_column_patterns(self, columns: List[str]) -> Dict[str, Any]:
        """Analyze column naming patterns"""
        patterns = {
            'case_types': {
                'lowercase': sum(1 for col in columns if col.islower()),
                'uppercase': sum(1 for col in columns if col.isupper()),
                'mixed_case': sum(1 for col in columns if not col.islower() and not col.isupper())
            },
            'separators': {
                'underscore': sum(1 for col in columns if '_' in col),
                'dash': sum(1 for col in columns if '-' in col),
                'space': sum(1 for col in columns if ' ' in col),
                'camelCase': sum(1 for col in columns if re.search(r'[a-z][A-Z]', col))
            },
            'special_chars': sum(1 for col in columns if re.search(r'[^a-zA-Z0-9_\-\s]', col)),
            'numeric_prefixes': sum(1 for col in columns if re.match(r'^\d', col))
        }
        return patterns
    
    # =============================================================================
    # HELPER METHODS - Data Quality Analysis
    # =============================================================================
    
    def _count_empty_strings(self, df: pd.DataFrame) -> Dict[str, int]:
        """Count empty strings in each column"""
        empty_counts = {}
        for col in df.columns:
            if df[col].dtype == 'object':
                empty_counts[col] = (df[col] == '').sum()
        return empty_counts
    
    def _detect_whitespace_issues(self, df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
        """Detect whitespace issues in string columns"""
        whitespace_issues = {}
        for col in df.columns:
            if df[col].dtype == 'object':
                series = df[col].astype(str)
                whitespace_issues[col] = {
                    'leading_spaces': (series != series.str.lstrip()).sum(),
                    'trailing_spaces': (series != series.str.rstrip()).sum(),
                    'multiple_spaces': series.str.contains(r'\s{2,}', na=False).sum()
                }
        return whitespace_issues
    
    def _detect_encoding_issues(self, df: pd.DataFrame) -> Dict[str, int]:
        """Detect potential encoding issues"""
        encoding_issues = {}
        for col in df.columns:
            if df[col].dtype == 'object':
                series = df[col].astype(str)
                # Look for common encoding artifacts from your config
                artifact_count = 0
                for artifact in ['Ã¡', 'Ã©', 'Ã­', 'Ã³', 'Ãº', 'Ã§', 'Ã£', '�']:
                    artifact_count += series.str.contains(re.escape(artifact), na=False).sum()
                encoding_issues[col] = artifact_count
        return encoding_issues
    
    def _find_numeric_with_text(self, df: pd.DataFrame) -> Dict[str, int]:
        """Find columns that should be numeric but contain text"""
        numeric_with_text = {}
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    numeric_converted = pd.to_numeric(df[col], errors='coerce')
                    non_numeric_count = numeric_converted.isnull().sum() - df[col].isnull().sum()
                    if non_numeric_count > 0:
                        numeric_with_text[col] = non_numeric_count
                except:
                    pass
        return numeric_with_text
    
    # =============================================================================
    # HELPER METHODS - Content Pattern Analysis
    # =============================================================================
    
    def _detect_brazilian_identifiers(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Detect Brazilian identifier patterns in column names"""
        identifiers = {
            'cnpj_columns': [],
            'cnes_columns': [],
            'ibge_columns': [],
            'cpf_columns': []
        }
        
        for col in df.columns:
            col_lower = col.lower()
            if 'cnpj' in col_lower:
                identifiers['cnpj_columns'].append(col)
            elif 'cnes' in col_lower:
                identifiers['cnes_columns'].append(col)
            elif 'ibge' in col_lower or 'codigo_municipio' in col_lower:
                identifiers['ibge_columns'].append(col)
            elif 'cpf' in col_lower:
                identifiers['cpf_columns'].append(col)
        
        return identifiers
    
    def _detect_geographic_columns(self, df: pd.DataFrame) -> List[str]:
        """Detect geographic-related columns"""
        geo_keywords = ['estado', 'municipio', 'cidade', 'uf', 'regiao', 'latitude', 'longitude', 'endereco']
        geo_columns = []
        
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in geo_keywords):
                geo_columns.append(col)
        
        return geo_columns
    
    def _detect_financial_columns(self, df: pd.DataFrame) -> List[str]:
        """Detect financial/monetary columns"""
        financial_keywords = ['valor', 'preco', 'custo', 'real', 'rs', 'brl', 'dinheiro', 'moeda']
        financial_columns = []
        
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in financial_keywords):
                financial_columns.append(col)
        
        return financial_columns
    
    def _detect_date_columns(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Detect date-related columns"""
        date_keywords = ['data', 'date', 'ano', 'mes', 'dia', 'periodo']
        date_info = {
            'potential_date_columns': [],
            'datetime_columns': []
        }
        
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in date_keywords):
                date_info['potential_date_columns'].append(col)
            if df[col].dtype in ['datetime64[ns]', 'datetime64[ns, UTC]']:
                date_info['datetime_columns'].append(col)
        
        return date_info
    
    def _identify_categorical_columns(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Identify potential categorical columns"""
        categorical_info = {
            'low_cardinality': [],      # < 50 unique values
            'medium_cardinality': [],   # 50-500 unique values
            'boolean_like': []          # 2 unique values
        }
        
        for col in df.columns:
            unique_count = df[col].nunique()
            if unique_count == 2:
                categorical_info['boolean_like'].append(col)
            elif unique_count < 50:
                categorical_info['low_cardinality'].append(col)
            elif unique_count < 500:
                categorical_info['medium_cardinality'].append(col)
        
        return categorical_info
    
    def _analyze_value_lengths(self, df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        """Analyze string length statistics for text columns"""
        length_stats = {}
        for col in df.columns:
            if df[col].dtype == 'object':
                lengths = df[col].astype(str).str.len()
                length_stats[col] = {
                    'min_length': lengths.min(),
                    'max_length': lengths.max(),
                    'mean_length': lengths.mean(),
                    'std_length': lengths.std()
                }
        return length_stats
    
    # =============================================================================
    # HELPER METHODS - Brazilian-Specific Analysis
    # =============================================================================
    
    def _analyze_cnpj_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze CNPJ patterns in the data"""
        cnpj_analysis = {}
        for col in df.columns:
            if 'cnpj' in col.lower() and df[col].dtype == 'object':
                series = df[col].astype(str)
                cnpj_analysis[col] = {
                    'formatted_count': series.str.match(r'\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}').sum(),
                    'numeric_only': series.str.match(r'^\d{14}$').sum(),
                    'invalid_format': len(series) - series.str.match(r'(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}|\d{14})').sum()
                }
        return cnpj_analysis
    
    def _analyze_cnes_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze CNES patterns in the data"""
        cnes_analysis = {}
        for col in df.columns:
            if 'cnes' in col.lower() and df[col].dtype == 'object':
                series = df[col].astype(str)
                cnes_analysis[col] = {
                    'seven_digits': series.str.match(r'^\d{7}$').sum(),
                    'other_formats': len(series) - series.str.match(r'^\d{7}$').sum()
                }
        return cnes_analysis
    
    def _analyze_ibge_codes(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze IBGE code patterns"""
        ibge_analysis = {}
        for col in df.columns:
            col_lower = col.lower()
            if 'ibge' in col_lower or 'codigo_municipio' in col_lower:
                if df[col].dtype in ['int64', 'float64', 'object']:
                    series = df[col].astype(str)
                    ibge_analysis[col] = {
                        'six_digits': series.str.match(r'^\d{6}$').sum(),
                        'seven_digits': series.str.match(r'^\d{7}$').sum(),
                        'other_formats': len(series) - series.str.match(r'^\d{6,7}$').sum()
                    }
        return ibge_analysis
    
    def _analyze_currency_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze Brazilian currency patterns"""
        currency_analysis = {}
        for col in df.columns:
            if df[col].dtype == 'object':
                series = df[col].astype(str)
                # Look for Brazilian currency patterns
                has_rs = series.str.contains(r'R\$', na=False).sum()
                has_comma_decimal = series.str.contains(r'\d+,\d{2}', na=False).sum()
                has_dot_thousands = series.str.contains(r'\d{1,3}\.\d{3}', na=False).sum()
                
                if has_rs > 0 or has_comma_decimal > 0 or has_dot_thousands > 0:
                    currency_analysis[col] = {
                        'rs_symbol': has_rs,
                        'comma_decimal': has_comma_decimal,
                        'dot_thousands': has_dot_thousands
                    }
        return currency_analysis
    
    def _analyze_coordinate_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze coordinate patterns for Brazilian geography"""
        coord_analysis = {}
        for col in df.columns:
            col_lower = col.lower()
            if any(coord_word in col_lower for coord_word in ['lat', 'lon', 'coord']):
                if df[col].dtype in ['float64', 'object']:
                    coord_analysis[col] = {
                        'valid_range': self._check_brazilian_coord_range(df[col], col_lower),
                        'decimal_places': self._analyze_decimal_precision(df[col])
                    }
        return coord_analysis
    
    def _check_brazilian_coord_range(self, series: pd.Series, col_name: str) -> Dict[str, int]:
        """Check if coordinates are within Brazilian geographic bounds"""
        try:
            numeric_series = pd.to_numeric(series, errors='coerce')
            if 'lat' in col_name:
                # Brazilian latitude range: approximately -33 to 5
                valid_range = ((numeric_series >= -35) & (numeric_series <= 6)).sum()
            elif 'lon' in col_name:
                # Brazilian longitude range: approximately -74 to -32
                valid_range = ((numeric_series >= -75) & (numeric_series <= -30)).sum()
            else:
                valid_range = 0
            
            return {
                'within_brazil_bounds': valid_range,
                'outside_bounds': len(numeric_series) - valid_range
            }
        except:
            return {'within_brazil_bounds': 0, 'outside_bounds': 0}
    
    def _analyze_decimal_precision(self, series: pd.Series) -> Dict[str, float]:
        """Analyze decimal precision in numeric data"""
        try:
            numeric_series = pd.to_numeric(series, errors='coerce').dropna()
            decimal_places = numeric_series.astype(str).str.extract(r'\.(\d+)')[0].str.len()
            return {
                'avg_decimal_places': decimal_places.mean(),
                'max_decimal_places': decimal_places.max()
            }
        except:
            return {'avg_decimal_places': 0, 'max_decimal_places': 0}
    
    def _detect_state_references(self, df: pd.DataFrame) -> List[str]:
        """Detect columns that might contain Brazilian state information"""
        state_columns = []
        state_keywords = ['estado', 'uf', 'sigla_uf']
        
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in state_keywords):
                state_columns.append(col)
        
        return state_columns
    
    def _detect_municipality_patterns(self, df: pd.DataFrame) -> List[str]:
        """Detect municipality-related columns"""
        municipality_columns = []
        municipality_keywords = ['municipio', 'cidade', 'nome_municipio']
        
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in municipality_keywords):
                municipality_columns.append(col)
        
        return municipality_columns
    
    def _get_sample_data(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Get representative sample data"""
        return {
            'filename': filename,
            'first_5_rows': df.head().to_dict('records'),
            'random_5_rows': df.sample(min(5, len(df))).to_dict('records') if len(df) > 0 else []
        }


class CSVLoaderAndAnalyzer:
    """
    Robust CSV loader with automatic encoding detection and Brazilian data handling
    Combined with the analyzer for complete exploration workflow
    """
    
    def __init__(self):
        self.config = get_config()
        self.logger = get_exploration_logger()
        self.analyzer = BrazilianHealthDataAnalyzer()
    
    def load_csv_robust(self, csv_path: str) -> pd.DataFrame:
        """
        Load CSV with automatic encoding detection and error handling for Brazilian data
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            Loaded DataFrame with cleaned text
        """
        csv_path = Path(csv_path)
        self.logger.info(f"📂 Loading CSV: {csv_path.name}")
        
        # Common encodings for Brazilian data
        encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'windows-1252', 'cp1252']
        separators_to_try = [',', ';', '\t']
        
        # Try different combinations of encoding and separators
        for encoding in encodings_to_try:
            for sep in separators_to_try:
                try:
                    self.logger.debug(f"   Trying {encoding} with '{sep}' separator")
                    df = pd.read_csv(
                        csv_path, 
                        encoding=encoding, 
                        sep=sep,
                        on_bad_lines='skip', 
                        engine='python'
                    )
                    
                    if df.shape[1] > 5:  # Reasonable column count
                        self.logger.info(f"   ✅ Loaded with {encoding} + '{sep}': {df.shape[0]:,} rows × {df.shape[1]} columns")
                        
                        # Clean encoding artifacts using config
                        df = self._clean_encoding_artifacts(df)
                        return df
                        
                except Exception as e:
                    self.logger.debug(f"   Failed with {encoding} + '{sep}': {str(e)}")
                    continue
        
        # If standard approaches fail, try the diagnostic approach from your original code
        self.logger.info("   🔧 Trying diagnostic approach...")
        try:
            return self._diagnose_and_load_csv(csv_path)
        except Exception as e:
            self.logger.error(f"   ❌ Diagnostic approach failed: {str(e)}")
        
        raise Exception(f"Could not load {csv_path} with any encoding/separator combination")
    
    def _diagnose_and_load_csv(self, csv_path: Path) -> pd.DataFrame:
        """
        Diagnose CSV structure and load with best configuration (from your original code)
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            Loaded DataFrame
        """
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'windows-1252']
        separators = [',', ';', '\t', '|']
        
        best_config = None
        max_columns = 0
        
        # Test different combinations to find the best one
        for encoding in encodings:
            for sep in separators:
                try:
                    # Read just the first few lines to test
                    test_df = pd.read_csv(csv_path, encoding=encoding, sep=sep, nrows=5)
                    
                    if test_df.shape[1] > max_columns:
                        max_columns = test_df.shape[1]
                        best_config = {
                            'encoding': encoding,
                            'separator': sep,
                            'columns': test_df.shape[1]
                        }
                        
                except Exception:
                    continue
        
        if best_config:
            self.logger.info(f"   📊 Best config: {best_config['encoding']} + '{best_config['separator']}' ({best_config['columns']} columns)")
            
            # Load with the best configuration
            df = pd.read_csv(
                csv_path,
                encoding=best_config['encoding'],
                sep=best_config['separator'],
                on_bad_lines='skip',
                engine='python'
            )
            
            return df
        else:
            raise Exception("Could not find any working configuration")
    
    def _clean_encoding_artifacts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean common encoding artifacts in Brazilian text data"""
        self.logger.debug("   🧹 Cleaning encoding artifacts...")
        
        # Use encoding fixes from config
        encoding_fixes = self.config.encoding_fixes
        
        # Apply fixes to text columns
        text_columns = df.select_dtypes(include=['object']).columns
        fixes_applied = 0
        
        for col in text_columns:
            original_series = df[col].astype(str)
            cleaned_series = original_series.copy()
            
            # Apply encoding fixes
            for old, new in encoding_fixes.items():
                cleaned_series = cleaned_series.str.replace(old, new, regex=False)
            
            # Count changes
            changes = (original_series != cleaned_series).sum()
            fixes_applied += changes
            df[col] = cleaned_series
        
        # Clean column names
        original_columns = df.columns.tolist()
        cleaned_columns = []
        
        for col in original_columns:
            cleaned_col = self._fix_column_name(str(col))
            cleaned_columns.append(cleaned_col)
        
        df.columns = cleaned_columns
        
        if fixes_applied > 0:
            self.logger.debug(f"   ✅ Fixed {fixes_applied:,} encoding artifacts")
        
        return df
    
    def _fix_column_name(self, col_name: str) -> str:
        """Fix encoding issues in column names using config"""
        encoding_fixes = self.config.encoding_fixes
        
        result = col_name
        for old, new in encoding_fixes.items():
            result = result.replace(old, new)
        
        # Remove problematic characters and normalize
        result = ''.join(c if c.isalnum() or c in '_-' else '_' for c in result)
        result = result.strip('_')
        
        return result
    
    @log_data_operation(get_exploration_logger(), "Complete CSV exploration")
    def explore_csv_file(self, csv_path: str) -> Dict[str, Any]:
        """
        Complete workflow: load CSV, analyze, and return comprehensive analysis
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            Comprehensive exploration analysis
        """
        csv_path = Path(csv_path)
        
        # Load CSV with robust handling
        df = self.load_csv_robust(csv_path)
        
        # Generate comprehensive analysis
        analysis = self.analyzer.generate_comprehensive_analysis(df, csv_path.name)
        
        return analysis


# Convenience functions for easy usage
def explore_health_csv(csv_path: str) -> Dict[str, Any]:
    """
    Simple function to explore a Brazilian health CSV file
    
    Args:
        csv_path: Path to CSV file
        
    Returns:
        Comprehensive exploration analysis
    """
    explorer = CSVLoaderAndAnalyzer()
    return explorer.explore_csv_file(csv_path)


def explore_all_health_csvs(csv_directory: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Explore all CSV files in a directory
    
    Args:
        csv_directory: Directory containing CSV files (default: config raw data dir)
        
    Returns:
        List of exploration analyses
    """
    config = get_config()
    logger = get_exploration_logger()
    
    if csv_directory is None:
        csv_directory = config.raw_data_dir
    else:
        csv_directory = Path(csv_directory)
    
    logger.info(f"🔍 Exploring all CSV files in: {csv_directory}")
    
    csv_files = list(csv_directory.glob("*.csv"))
    if not csv_files:
        logger.warning(f"⚠️  No CSV files found in {csv_directory}")
        return []
    
    logger.info(f"📋 Found {len(csv_files)} CSV files to explore")
    
    explorer = CSVLoaderAndAnalyzer()
    analyses = []
    
    for csv_file in csv_files:
        try:
            analysis = explorer.explore_csv_file(csv_file)
            analyses.append(analysis)
        except Exception as e:
            logger.error(f"❌ Failed to explore {csv_file.name}: {str(e)}")
    
    logger.info(f"✅ Completed exploration of {len(analyses)}/{len(csv_files)} files")
    return analyses


# Example usage and testing
if __name__ == "__main__":
    # Test the exploration module
    print("🔍 Fair-Price Data Exploration Module")
    print("=" * 60)
    
    config = get_config()
    
    # Check if we have any CSV files to explore
    csv_files = list(config.raw_data_dir.glob("*.csv"))
    
    if csv_files:
        print(f"📋 Found {len(csv_files)} CSV files in {config.raw_data_dir}")
        
        # Test with the first file
        test_file = csv_files[0]
        print(f"🧪 Testing exploration with: {test_file.name}")
        
        try:
            analysis = explore_health_csv(test_file)
            
            # Print summary
            schema = analysis['schema']
            quality = analysis['quality']
            
            print(f"\n📊 EXPLORATION SUMMARY:")
            print(f"   Shape: {schema['shape'][0]:,} rows × {schema['shape'][1]} columns")
            print(f"   Memory: {schema['memory_usage'] / 1024 / 1024:.2f} MB")
            
            completeness = (1 - sum(quality['missing_values'].values()) / (schema['row_count'] * schema['column_count'])) * 100
            print(f"   Data completeness: {completeness:.1f}%")
            print(f"   Duplicate rows: {quality['duplicate_rows']:,}")
            
            print(f"\n✅ Exploration test completed successfully!")
            
        except Exception as e:
            print(f"❌ Exploration test failed: {str(e)}")
    else:
        print(f"⚠️  No CSV files found in {config.raw_data_dir}")
        print("💡 Run the extraction module first to download CSV files")