"""
Simple standardization processor that combines cleaning and validation.
Focused on core functionality without over-engineering.
"""

import pandas as pd
import time
from pathlib import Path
from typing import Dict, Any, Tuple


class SimpleStandardizationProcessor:
    """
    Simple processor for standardizing Brazilian health procurement data.
    Combines cleaning, validation, and basic reporting.
    """
    
    def __init__(self, config, logger):
        """Initialize with config and logger."""
        self.config = config
        self.logger = logger
    
    def load_raw_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Load a raw CSV file with Brazilian encoding handling.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Loaded DataFrame
        """
        self.logger.info(f"Loading {file_path.name}...")
        
        try:
            # Try primary encoding first (latin-1 with semicolon)
            df = pd.read_csv(file_path, encoding='latin-1', sep=';')
            self.logger.info(f"Loaded {len(df):,} rows with latin-1 encoding")
            
        except Exception as e:
            self.logger.warning(f"Primary encoding failed, trying fallbacks...")
            
            # Try alternative configurations
            fallback_configs = [
                {'encoding': 'utf-8', 'sep': ';'},
                {'encoding': 'latin-1', 'sep': ','},
                {'encoding': 'utf-8', 'sep': ','},
            ]
            
            df = None
            for config in fallback_configs:
                try:
                    df = pd.read_csv(file_path, **config)
                    self.logger.info(f"Loaded with {config}")
                    break
                except Exception:
                    continue
            
            if df is None:
                raise Exception(f"Failed to load {file_path.name} with any encoding")
        
        return df
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply comprehensive cleaning to a DataFrame.
        
        Args:
            df: Input DataFrame to clean
            
        Returns:
            Cleaned DataFrame
        """
        self.logger.info("Applying data cleaning...")
        
        # Copy for safety
        df_clean = df.copy()
        
        # 1. Standardize column names (ASCII-only)
        df_clean = self._standardize_column_names(df_clean)
        
        # 2. Clean text columns (encoding fixes)
        text_columns = [col for col in df_clean.columns 
                       if any(keyword in col.lower() for keyword in 
                             ['descricao', 'fabricante', 'fornecedor', 'instituicao', 'municipio'])]
        df_clean = self._clean_text_columns(df_clean, text_columns)
        
        # 3. Clean CNPJ columns
        cnpj_columns = [col for col in df_clean.columns if 'cnpj' in col.lower()]
        df_clean = self._clean_cnpj_columns(df_clean, cnpj_columns)
        
        # 4. Clean currency columns
        currency_columns = [col for col in df_clean.columns if 'preco' in col.lower()]
        df_clean = self._clean_currency_columns(df_clean, currency_columns)
        
        self.logger.info(f"Cleaning complete: {len(df_clean)} rows, {len(df_clean.columns)} columns")
        
        return df_clean
    
    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add validation flags and quality score to DataFrame.
        
        Args:
            df: Cleaned DataFrame
            
        Returns:
            DataFrame with validation columns added
        """
        self.logger.info("Running validation and quality scoring...")
        
        df_validated = df.copy()
        
        # Add basic validation flags
        df_validated['has_valid_cnpj'] = df_validated.apply(self._check_valid_cnpj, axis=1)
        df_validated['has_valid_state'] = df_validated.apply(self._check_valid_state, axis=1)
        df_validated['has_valid_year'] = df_validated.apply(self._check_valid_year, axis=1)
        df_validated['has_positive_prices'] = df_validated.apply(self._check_positive_prices, axis=1)
        
        # Calculate overall quality score (0-100)
        df_validated['quality_score'] = df_validated.apply(self._calculate_quality_score, axis=1)
        
        avg_quality = df_validated['quality_score'].mean()
        self.logger.info(f"Validation complete: {avg_quality:.1f}% average quality score")
        
        return df_validated
    
    def standardize_file(self, input_path: Path, output_path: Path) -> Dict[str, Any]:
        """
        Complete standardization pipeline for one file.
        
        Args:
            input_path: Raw CSV file path
            output_path: Output path for standardized file
            
        Returns:
            Processing report
        """
        start_time = time.time()
        file_name = input_path.name
        
        self.logger.info(f"Starting standardization of {file_name}")
        
        # Load, clean, validate
        df_raw = self.load_raw_csv(input_path)
        df_clean = self.clean_dataframe(df_raw)
        df_validated = self.validate_dataframe(df_clean)
        
        # Remove validation columns for output (keep only business data)
        business_columns = [col for col in df_validated.columns 
                          if not col.startswith(('quality_', 'has_valid_'))]
        df_output = df_validated[business_columns]
        
        # Save standardized file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_output.to_csv(output_path, index=False, encoding='utf-8', sep=',')
        
        processing_time = time.time() - start_time
        
        # Generate report
        report = {
            'file_name': file_name,
            'rows_input': len(df_raw),
            'rows_output': len(df_output),
            'columns_input': len(df_raw.columns),
            'columns_output': len(df_output.columns),
            'average_quality_score': df_validated['quality_score'].mean(),
            'high_quality_rows': len(df_validated[df_validated['quality_score'] >= 80]),
            'processing_time_seconds': processing_time,
            'input_file': str(input_path),
            'output_file': str(output_path)
        }
        
        self.logger.info(f"Completed {file_name}: {report['average_quality_score']:.1f}% quality ({processing_time:.2f}s)")
        
        return report
    
    # Helper methods for cleaning (simplified versions)
    def _standardize_column_names(self, df):
        """Convert column names to ASCII lowercase with underscores."""
        char_map = {'ã': 'a', 'á': 'a', 'â': 'a', 'à': 'a', 'é': 'e', 'ê': 'e', 
                   'í': 'i', 'ó': 'o', 'ô': 'o', 'õ': 'o', 'ú': 'u', 'ç': 'c'}
        
        new_columns = []
        for col in df.columns:
            clean_col = str(col).lower().strip()
            for pt_char, ascii_char in char_map.items():
                clean_col = clean_col.replace(pt_char, ascii_char)
            # Replace non-alphanumeric with underscores
            import re
            clean_col = re.sub(r'[^\w]', '_', clean_col)
            clean_col = re.sub(r'_+', '_', clean_col).strip('_')
            new_columns.append(clean_col)
        
        df.columns = new_columns
        return df
    
    def _clean_text_columns(self, df, text_columns):
        """Apply encoding fixes to text columns."""
        encoding_fixes = getattr(self.config, 'encoding_fixes', {})
        
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: self._fix_encoding(x, encoding_fixes))
        
        return df
    
    def _fix_encoding(self, text, encoding_fixes):
        """Fix encoding artifacts in text."""
        if pd.isna(text) or not isinstance(text, str):
            return text
        
        fixed_text = text
        for corrupted, correct in encoding_fixes.items():
            fixed_text = fixed_text.replace(corrupted, correct)
        
        return fixed_text
    
    def _clean_cnpj_columns(self, df, cnpj_columns):
        """Standardize CNPJ format to 14 digits."""
        for col in cnpj_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._standardize_cnpj)
        
        return df
    
    def _standardize_cnpj(self, cnpj):
        """Extract 14 digits from CNPJ."""
        if pd.isna(cnpj) or not isinstance(cnpj, str):
            return None
        
        import re
        digits_only = re.sub(r'\D', '', cnpj)
        return digits_only if len(digits_only) == 14 else None
    
    def _clean_currency_columns(self, df, currency_columns):
        """Convert Brazilian currency to float."""
        for col in currency_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_currency)
        
        return df
    
    def _clean_currency(self, value):
        """Convert currency string to float."""
        if pd.isna(value) or not isinstance(value, str):
            return None
        
        import re
        # Remove currency symbols
        clean_value = re.sub(r'[R$\s]', '', str(value))
        
        # Handle Brazilian decimal format (comma as decimal)
        if ',' in clean_value and '.' in clean_value:
            # Both present - periods are thousands, comma is decimal
            parts = clean_value.split('.')
            thousands_part = ''.join(parts[:-1])
            decimal_part = parts[-1]
            if ',' in decimal_part:
                decimal_split = decimal_part.split(',')
                if len(decimal_split) == 2:
                    clean_value = thousands_part + decimal_split[0] + '.' + decimal_split[1]
        elif ',' in clean_value:
            # Only comma - decimal separator
            clean_value = clean_value.replace(',', '.')
        
        try:
            return float(clean_value)
        except ValueError:
            return None
    
    # Validation helper methods
    def _check_valid_cnpj(self, row):
        """Check if row has at least one valid CNPJ."""
        cnpj_fields = [col for col in row.index if 'cnpj' in col.lower()]
        
        for field in cnpj_fields:
            if not pd.isna(row[field]) and self._validate_cnpj(str(row[field])):
                return True
        
        return False
    
    def _validate_cnpj(self, cnpj):
        """Validate CNPJ checksum."""
        if not cnpj or len(cnpj) != 14 or not cnpj.isdigit() or len(set(cnpj)) == 1:
            return False
        
        # Checksum validation
        weights_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum_1 = sum(int(cnpj[i]) * weights_1[i] for i in range(12))
        digit_1 = 0 if (sum_1 % 11) < 2 else 11 - (sum_1 % 11)
        
        if int(cnpj[12]) != digit_1:
            return False
        
        weights_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        sum_2 = sum(int(cnpj[i]) * weights_2[i] for i in range(13))
        digit_2 = 0 if (sum_2 % 11) < 2 else 11 - (sum_2 % 11)
        
        return int(cnpj[13]) == digit_2
    
    def _check_valid_state(self, row):
        """Check if state code is valid."""
        if 'uf' in row.index and not pd.isna(row['uf']):
            valid_states = {'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
                           'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
                           'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'}
            return str(row['uf']).upper().strip() in valid_states
        
        return False
    
    def _check_valid_year(self, row):
        """Check if year is in valid range."""
        if 'ano' in row.index and not pd.isna(row['ano']):
            try:
                year = int(row['ano'])
                return 2010 <= year <= 2030
            except (ValueError, TypeError):
                return False
        
        return False
    
    def _check_positive_prices(self, row):
        """Check if price fields are positive."""
        price_fields = [col for col in row.index if 'preco' in col.lower()]
        
        for field in price_fields:
            if not pd.isna(row[field]):
                try:
                    value = float(row[field])
                    if value > 0:
                        return True
                except (ValueError, TypeError):
                    continue
        
        return False
    
    def _calculate_quality_score(self, row):
        """Calculate quality score (0-100) for a row."""
        checks = [
            self._check_valid_cnpj(row),
            self._check_valid_state(row),
            self._check_valid_year(row),
            self._check_positive_prices(row)
        ]
        
        # Add required field checks
        required_fields = ['ano', 'uf'] + [col for col in row.index if 'descricao' in col.lower()]
        for field in required_fields:
            if field in row.index:
                checks.append(not pd.isna(row[field]) and str(row[field]).strip() != '')
        
        if not checks:
            return 0
        
        return (sum(checks) / len(checks)) * 100