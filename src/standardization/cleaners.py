"""
Data cleaning functions for Brazilian health procurement data standardization.

This module provides functions to clean and standardize raw CSV data from 
OpenDataSUS, focusing on Brazilian-specific data issues like encoding 
artifacts, CNPJ formats, and currency representations.
"""

import re
import pandas as pd
from typing import Dict, List, Any, Optional
from pathlib import Path


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names by removing special characters and normalizing case.
    
    Args:
        df: Input DataFrame with potentially inconsistent column names
        
    Returns:
        DataFrame with standardized column names
        
    Example:
        >>> df = pd.DataFrame({"Ano  ": [2020], "Código BR": [123]})
        >>> result = standardize_column_names(df)
        >>> list(result.columns)
        ['ano', 'codigo_br']
    """
    df_clean = df.copy()
    
    # Mapping for Portuguese characters to ASCII
    char_map = {
        'ã': 'a', 'á': 'a', 'â': 'a', 'à': 'a',
        'é': 'e', 'ê': 'e', 'è': 'e',
        'í': 'i', 'î': 'i', 'ì': 'i',
        'ó': 'o', 'ô': 'o', 'õ': 'o', 'ò': 'o',
        'ú': 'u', 'û': 'u', 'ù': 'u',
        'ç': 'c', 'ñ': 'n'
    }
    
    # Clean column names
    new_columns = []
    for col in df_clean.columns:
        # Convert to lowercase
        clean_col = str(col).lower()
        
        # Remove extra whitespace
        clean_col = clean_col.strip()
        
        # Replace Portuguese characters with ASCII equivalents
        for pt_char, ascii_char in char_map.items():
            clean_col = clean_col.replace(pt_char, ascii_char)
        
        # Replace spaces and special chars with underscores
        clean_col = re.sub(r'[^\w]', '_', clean_col)
        
        # Remove multiple underscores
        clean_col = re.sub(r'_+', '_', clean_col)
        
        # Remove leading/trailing underscores
        clean_col = clean_col.strip('_')
        
        new_columns.append(clean_col)
    
    df_clean.columns = new_columns
    return df_clean


def fix_brazilian_encoding(text: str, encoding_fixes: Dict[str, str]) -> str:
    """
    Fix Brazilian text encoding artifacts using predefined mappings.
    
    Args:
        text: Input text with potential encoding issues
        encoding_fixes: Dictionary mapping corrupted text to correct text
        
    Returns:
        Text with encoding artifacts fixed
        
    Example:
        >>> fixes = {"Ã§Ã": "ÇÃ", "Ã§": "ç", "ÃO": "O"}
        >>> fix_brazilian_encoding("MEDICAÃ§ÃO", fixes)
        'MEDICAÇÃO'
    """
    if pd.isna(text) or not isinstance(text, str):
        return text
    
    fixed_text = text
    # Apply fixes in order of longest match first to avoid double replacements
    sorted_fixes = sorted(encoding_fixes.items(), key=lambda x: len(x[0]), reverse=True)
    
    for corrupted, correct in sorted_fixes:
        fixed_text = fixed_text.replace(corrupted, correct)
    
    return fixed_text


def clean_text_columns(df: pd.DataFrame, text_columns: List[str], 
                      encoding_fixes: Dict[str, str]) -> pd.DataFrame:
    """
    Apply encoding fixes to specified text columns.
    
    Args:
        df: Input DataFrame
        text_columns: List of column names to clean
        encoding_fixes: Dictionary of encoding corrections
        
    Returns:
        DataFrame with cleaned text columns
    """
    df_clean = df.copy()
    
    for col in text_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(
                lambda x: fix_brazilian_encoding(x, encoding_fixes)
            )
    
    return df_clean


def standardize_cnpj(cnpj: str) -> Optional[str]:
    """
    Standardize CNPJ format to digits only (14 characters).
    
    Args:
        cnpj: CNPJ string in various formats
        
    Returns:
        Standardized CNPJ (digits only) or None if invalid
        
    Example:
        >>> standardize_cnpj("12.345.678/0001-90")
        '12345678000190'
        >>> standardize_cnpj("12345678000190")
        '12345678000190'
        >>> standardize_cnpj("invalid")
        None
    """
    if pd.isna(cnpj) or not isinstance(cnpj, str):
        return None
    
    # Remove all non-digit characters
    digits_only = re.sub(r'\D', '', cnpj)
    
    # CNPJ must have exactly 14 digits
    if len(digits_only) == 14:
        return digits_only
    
    return None


def clean_cnpj_columns(df: pd.DataFrame, cnpj_columns: List[str]) -> pd.DataFrame:
    """
    Clean and standardize all CNPJ columns in the DataFrame.
    
    Args:
        df: Input DataFrame
        cnpj_columns: List of CNPJ column names
        
    Returns:
        DataFrame with standardized CNPJ columns
    """
    df_clean = df.copy()
    
    for col in cnpj_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(standardize_cnpj)
    
    return df_clean


def clean_currency_value(value: str) -> Optional[float]:
    """
    Convert Brazilian currency strings to float values.
    
    Args:
        value: Currency string (e.g., "R$ 1.234,56", "1234,56")
        
    Returns:
        Float value or None if conversion fails
        
    Example:
        >>> clean_currency_value("R$ 1.234,56")
        1234.56
        >>> clean_currency_value("1234,56")
        1234.56
        >>> clean_currency_value("invalid")
        None
    """
    if pd.isna(value) or not isinstance(value, str):
        return None
    
    # Remove currency symbols and whitespace
    clean_value = re.sub(r'[R$\s]', '', str(value))
    
    # Handle Brazilian number format (comma as decimal separator)
    if ',' in clean_value:
        # Check if comma is decimal separator (no digits after thousands separator)
        parts = clean_value.split('.')
        if len(parts) > 1:
            # Has both periods and comma - periods are thousands separators
            thousands_part = ''.join(parts[:-1])  # Remove all periods
            decimal_part = parts[-1]
            if ',' in decimal_part:
                # Comma is decimal separator
                decimal_split = decimal_part.split(',')
                if len(decimal_split) == 2:
                    clean_value = thousands_part + decimal_split[0] + '.' + decimal_split[1]
        else:
            # Only comma, treat as decimal separator
            clean_value = clean_value.replace(',', '.')
    
    # Remove any remaining non-digit/non-decimal characters
    clean_value = re.sub(r'[^\d.]', '', clean_value)
    
    try:
        return float(clean_value)
    except ValueError:
        return None


def clean_currency_columns(df: pd.DataFrame, currency_columns: List[str]) -> pd.DataFrame:
    """
    Clean and convert currency columns to float values.
    
    Args:
        df: Input DataFrame
        currency_columns: List of currency column names
        
    Returns:
        DataFrame with cleaned currency columns
    """
    df_clean = df.copy()
    
    for col in currency_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(clean_currency_value)
    
    return df_clean


def clean_dataframe(df: pd.DataFrame, config: Any) -> pd.DataFrame:
    """
    Apply comprehensive cleaning to a DataFrame using configuration settings.
    
    Args:
        df: Input DataFrame to clean
        config: Configuration object with cleaning settings
        
    Returns:
        Fully cleaned DataFrame
    """
    # Start with column name standardization
    df_clean = standardize_column_names(df)
    
    # Apply text encoding fixes
    text_columns = [col for col in df_clean.columns 
                   if col in ['descricao_catmat', 'fabricante', 'fornecedor', 
                            'nome_instituicao', 'municipio_instituicao']]
    df_clean = clean_text_columns(df_clean, text_columns, config.encoding_fixes)
    
    # Clean CNPJ columns
    cnpj_columns = [col for col in df_clean.columns 
                   if 'cnpj' in col.lower()]
    df_clean = clean_cnpj_columns(df_clean, cnpj_columns)
    
    # Clean currency columns
    currency_columns = [col for col in df_clean.columns 
                       if 'preco' in col.lower()]
    df_clean = clean_currency_columns(df_clean, currency_columns)
    
    return df_clean