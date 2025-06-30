"""
Brazilian-specific data validation functions for health procurement data.

This module provides validation functions for Brazilian identifiers (CNPJ, CNES),
geographic codes, and business rules specific to Brazilian health system data.
"""

import re
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple


def validate_cnpj(cnpj: str) -> bool:
    """
    Validate Brazilian CNPJ using checksum algorithm.
    
    Args:
        cnpj: CNPJ string (14 digits expected)
        
    Returns:
        True if CNPJ is valid, False otherwise
        
    Example:
        >>> validate_cnpj("11222333000181")  # Valid CNPJ
        True
        >>> validate_cnpj("12345678000190")  # Invalid checksum
        False
    """
    if not cnpj or not isinstance(cnpj, str) or len(cnpj) != 14:
        return False
    
    # Check if all characters are digits
    if not cnpj.isdigit():
        return False
    
    # CNPJ with all same digits is invalid
    if len(set(cnpj)) == 1:
        return False
    
    # Calculate first check digit
    weights_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    sum_1 = sum(int(cnpj[i]) * weights_1[i] for i in range(12))
    remainder_1 = sum_1 % 11
    digit_1 = 0 if remainder_1 < 2 else 11 - remainder_1
    
    if int(cnpj[12]) != digit_1:
        return False
    
    # Calculate second check digit
    weights_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    sum_2 = sum(int(cnpj[i]) * weights_2[i] for i in range(13))
    remainder_2 = sum_2 % 11
    digit_2 = 0 if remainder_2 < 2 else 11 - remainder_2
    
    return int(cnpj[13]) == digit_2


def validate_brazilian_state(uf: str) -> bool:
    """
    Validate Brazilian state code (UF).
    
    Args:
        uf: Two-letter state code
        
    Returns:
        True if valid Brazilian state code, False otherwise
    """
    if not uf or not isinstance(uf, str):
        return False
    
    valid_states = {
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
        'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
        'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    }
    
    return uf.upper().strip() in valid_states


def validate_year_range(year: Any, min_year: int = 2010, max_year: int = 2030) -> bool:
    """
    Validate year is within reasonable range for health data.
    
    Args:
        year: Year value to validate
        min_year: Minimum acceptable year
        max_year: Maximum acceptable year
        
    Returns:
        True if year is valid, False otherwise
    """
    try:
        year_int = int(year)
        return min_year <= year_int <= max_year
    except (ValueError, TypeError):
        return False


def validate_positive_number(value: Any) -> bool:
    """
    Validate that a value is a positive number.
    
    Args:
        value: Value to validate
        
    Returns:
        True if positive number, False otherwise
    """
    try:
        num_value = float(value)
        return num_value > 0 and not pd.isna(num_value)
    except (ValueError, TypeError):
        return False


def validate_required_fields(row: pd.Series, required_fields: List[str]) -> Dict[str, bool]:
    """
    Validate that required fields are not null/empty.
    
    Args:
        row: DataFrame row to validate
        required_fields: List of field names that are required
        
    Returns:
        Dictionary mapping field names to validation results
    """
    results = {}
    
    for field in required_fields:
        if field in row.index:
            value = row[field]
            # Check for null, empty string, or whitespace-only
            is_valid = (
                not pd.isna(value) and 
                value is not None and
                str(value).strip() != ''
            )
            results[field] = is_valid
        else:
            results[field] = False
    
    return results


def calculate_row_quality_score(row: pd.Series, validation_rules: Dict[str, Any]) -> float:
    """
    Calculate a quality score (0-100) for a data row based on validation rules.
    
    Args:
        row: DataFrame row to score
        validation_rules: Dictionary defining validation rules
        
    Returns:
        Quality score from 0 to 100
    """
    total_checks = 0
    passed_checks = 0
    
    # Required fields validation
    if 'required_fields' in validation_rules:
        required_results = validate_required_fields(row, validation_rules['required_fields'])
        total_checks += len(required_results)
        passed_checks += sum(required_results.values())
    
    # CNPJ validations
    cnpj_fields = ['cnpj_fabricante', 'cnpj_fornecedor', 'cnpj_instituicao']
    for field in cnpj_fields:
        if field in row.index and not pd.isna(row[field]):
            total_checks += 1
            if validate_cnpj(str(row[field])):
                passed_checks += 1
    
    # State validation
    if 'uf' in row.index and not pd.isna(row['uf']):
        total_checks += 1
        if validate_brazilian_state(str(row['uf'])):
            passed_checks += 1
    
    # Year validation
    if 'ano' in row.index and not pd.isna(row['ano']):
        total_checks += 1
        if validate_year_range(row['ano']):
            passed_checks += 1
    
    # Positive number validations
    numeric_fields = ['qtd_itens_comprados', 'preco_unitario', 'preco_total']
    for field in numeric_fields:
        if field in row.index and not pd.isna(row[field]):
            total_checks += 1
            if validate_positive_number(row[field]):
                passed_checks += 1
    
    # Calculate percentage score
    if total_checks == 0:
        return 0.0
    
    return (passed_checks / total_checks) * 100


def validate_dataframe(df: pd.DataFrame, validation_rules: Dict[str, Any]) -> pd.DataFrame:
    """
    Validate an entire DataFrame and add quality scores.
    
    Args:
        df: DataFrame to validate
        validation_rules: Dictionary defining validation rules
        
    Returns:
        DataFrame with added validation columns
    """
    df_validated = df.copy()
    
    # Calculate quality score for each row
    df_validated['quality_score'] = df_validated.apply(
        lambda row: calculate_row_quality_score(row, validation_rules), 
        axis=1
    )
    
    # Add overall validation flags
    df_validated['has_valid_cnpj'] = df_validated.apply(
        lambda row: any(
            validate_cnpj(str(row[field])) 
            for field in ['cnpj_fabricante', 'cnpj_fornecedor', 'cnpj_instituicao']
            if field in row.index and not pd.isna(row[field])
        ), 
        axis=1
    )
    
    df_validated['has_valid_state'] = df_validated['uf'].apply(
        lambda x: validate_brazilian_state(str(x)) if not pd.isna(x) else False
    )
    
    df_validated['has_valid_year'] = df_validated['ano'].apply(
        lambda x: validate_year_range(x) if not pd.isna(x) else False
    )
    
    return df_validated


def get_validation_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate a summary of validation results for a DataFrame.
    
    Args:
        df: Validated DataFrame (with quality_score column)
        
    Returns:
        Dictionary with validation summary statistics
    """
    if 'quality_score' not in df.columns:
        return {'error': 'DataFrame must be validated first (missing quality_score column)'}
    
    summary = {
        'total_rows': len(df),
        'avg_quality_score': df['quality_score'].mean(),
        'min_quality_score': df['quality_score'].min(),
        'max_quality_score': df['quality_score'].max(),
        'high_quality_rows': len(df[df['quality_score'] >= 90]),
        'medium_quality_rows': len(df[(df['quality_score'] >= 70) & (df['quality_score'] < 90)]),
        'low_quality_rows': len(df[df['quality_score'] < 70]),
        'quality_distribution': {
            '90-100%': len(df[df['quality_score'] >= 90]),
            '70-89%': len(df[(df['quality_score'] >= 70) & (df['quality_score'] < 90)]),
            '50-69%': len(df[(df['quality_score'] >= 50) & (df['quality_score'] < 70)]),
            '0-49%': len(df[df['quality_score'] < 50])
        }
    }
    
    # Add validation flags summary if available
    validation_flags = ['has_valid_cnpj', 'has_valid_state', 'has_valid_year']
    for flag in validation_flags:
        if flag in df.columns:
            summary[f'{flag}_count'] = df[flag].sum()
            summary[f'{flag}_percentage'] = (df[flag].sum() / len(df)) * 100
    
    return summary