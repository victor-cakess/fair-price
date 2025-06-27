# CSV Exploration Framework Documentation

## Project: Arcade - Brazilian Health Economics Data Pipeline
### Module: CSV Data Exploration and Analysis

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Functions](#core-functions)
4. [Usage Guide](#usage-guide)
5. [Implementation Details](#implementation-details)
6. [Brazilian-Specific Features](#brazilian-specific-features)
7. [Troubleshooting](#troubleshooting)
8. [Example Outputs](#example-outputs)

---

## Overview

This CSV exploration framework was developed as part of the Arcade Brazilian Health Economics Data Pipeline project. It provides comprehensive analysis capabilities for Brazilian government CSV data, with specialized handling for encoding issues, structural inconsistencies, and Brazilian-specific data patterns.

### Key Objectives

- **Comprehensive Analysis**: Complete schema, quality, and content analysis of CSV files
- **Brazilian Data Focus**: Specialized handling for Portuguese text, Brazilian identifiers (CNPJ, CNES, IBGE), and currency formats
- **Robust Data Loading**: Automatic encoding detection and error handling for real-world government data
- **Standardization Preparation**: Identify inconsistencies across files to prepare for database design
- **Documentation Generation**: Automated text report generation for findings and recommendations

### Problem Statement

Brazilian government CSV data often suffers from:
- **Encoding Issues**: UTF-8 text incorrectly read as Latin-1, creating artifacts like `Ã§` instead of `ç`
- **Structural Inconsistencies**: Variable field counts, different separators across files
- **Data Quality Issues**: Missing values, duplicate rows, mixed data types
- **Evolution Over Time**: Schema changes across years requiring cross-file analysis

---

## Architecture

### Design Philosophy

The framework follows a **modular, function-based architecture** with clear separation of concerns:

```
┌─────────────────────────────────────────┐
│             CSV File Input              │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│          Diagnosis & Loading            │
│  • Encoding Detection                   │
│  • Separator Detection                  │
│  • Error Handling                       │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│         Text Cleaning                   │
│  • Encoding Artifact Removal           │
│  • Portuguese Character Fixes          │
│  • Column Name Normalization           │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│        Comprehensive Analysis           │
│  • Schema Analysis                      │
│  • Data Quality Assessment             │
│  • Content Pattern Detection           │
│  • Brazilian-Specific Analysis         │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│       Report Generation                 │
│  • Individual File Reports              │
│  • Cross-File Comparison               │
│  • Standardization Recommendations     │
└─────────────────────────────────────────┘
```

### Core Components

1. **Data Loading Layer**: Robust CSV loading with encoding detection
2. **Text Processing Layer**: Encoding artifact cleaning and normalization
3. **Analysis Engine**: Multi-dimensional data analysis
4. **Reporting Layer**: Automated documentation generation
5. **Comparison Engine**: Cross-file analysis for schema evolution

---

## Core Functions

### Primary Workflow Functions

#### `explore_csv_and_generate_report(csv_path, output_dir)`
**Purpose**: Complete end-to-end workflow for single CSV analysis
- Diagnoses CSV structure and encoding
- Loads data with optimal configuration
- Cleans encoding artifacts
- Performs comprehensive analysis
- Generates detailed text report

**Parameters:**
- `csv_path`: Path to CSV file to analyze
- `output_dir`: Directory to save generated reports

**Returns:** Dictionary containing complete exploration summary

#### `generate_comparison_report(summaries, output_path)`
**Purpose**: Cross-file analysis and comparison
- Analyzes column consistency across files
- Tracks schema evolution over time
- Identifies data type inconsistencies
- Generates standardization recommendations

### Data Loading Functions

#### `diagnose_csv_structure(csv_path)`
**Purpose**: Intelligent CSV structure detection
- Tests multiple encoding/separator combinations
- Identifies optimal configuration for maximum column detection
- Provides diagnostic information about file structure

**Key Features:**
- Tests encodings: UTF-8, Latin-1, ISO-8859-1, Windows-1252
- Tests separators: comma, semicolon, tab, pipe
- Returns best configuration based on column count

#### `_load_csv_with_encoding(csv_path)`
**Purpose**: Robust CSV loading with error handling
- Progressive fallback through multiple encoding attempts
- Handles structural inconsistencies (variable field counts)
- Line-by-line reconstruction as last resort

**Error Handling Strategy:**
1. Standard pandas loading with different encodings
2. Flexible parsing with bad line skipping
3. Aggressive parsing with auto-detection
4. Manual line-by-line reconstruction

### Text Cleaning Functions

#### `_clean_encoding_artifacts(df)`
**Purpose**: Comprehensive encoding artifact cleanup
- Fixes Portuguese character encoding issues
- Cleans both data content and column names
- Removes unwanted characters and artifacts

#### `_fix_common_portuguese_chars(series)`
**Purpose**: Targeted Portuguese text cleaning
- Handles specific character encoding artifacts
- Recognizes common Portuguese word patterns
- Fixes whole-word replacements (e.g., `institui��o` → `instituição`)

#### `_fix_column_name(col_name)`
**Purpose**: Column name standardization
- Removes encoding artifacts from column names
- Normalizes to ASCII-safe characters
- Standardizes naming conventions

### Analysis Functions

#### `analyze_schema(df, filename)`
**Purpose**: Complete schema analysis
- Column inventory and data types
- Naming pattern analysis
- Memory usage assessment
- Duplicate detection

#### `analyze_data_quality(df, filename)`
**Purpose**: Comprehensive data quality assessment
- Missing value analysis
- Duplicate row detection
- Encoding issue identification
- Data type validation

#### `analyze_content_patterns(df, filename)`
**Purpose**: Content pattern recognition
- Categorical column identification
- Geographic data detection
- Financial data recognition
- Date column analysis

#### `analyze_brazilian_specifics(df, filename)`
**Purpose**: Brazilian data pattern analysis
- CNPJ format validation
- CNES code analysis
- IBGE code detection
- Currency pattern recognition
- Geographic coordinate validation

### Report Generation Functions

#### `generate_txt_report(summary, output_path)`
**Purpose**: Structured text report generation
- Multi-section detailed analysis
- Human-readable formatting
- Actionable recommendations
- Sample data inclusion

#### `compare_schemas_across_files(summaries)`
**Purpose**: Cross-file schema comparison
- Column consistency analysis
- Schema evolution tracking
- Data type consistency validation
- Brazilian pattern consistency

---

## Usage Guide

### Basic Single File Analysis

```python
# Analyze a single CSV file
summary = explore_csv_and_generate_report('2020.csv', 'reports/')

# Output: exploration_report_2020.txt
```

### Complete Multi-File Analysis

```python
# Analyze all CSV files
summaries = []
for year in ['2020', '2021', '2022', '2023', '2024']:
    summary = explore_csv_and_generate_report(f'{year}.csv', 'reports/')
    summaries.append(summary)

# Generate cross-file comparison
generate_comparison_report(summaries, 'reports/comparison_report.txt')
```

### Advanced Usage

```python
# Just load and clean a CSV
df = _load_csv_with_encoding('problematic_file.csv')
df_clean = _clean_encoding_artifacts(df)

# Generate individual analysis components
schema_info = analyze_schema(df_clean, 'file.csv')
quality_info = analyze_data_quality(df_clean, 'file.csv')
brazilian_info = analyze_brazilian_specifics(df_clean, 'file.csv')
```

---

## Implementation Details

### Encoding Detection Strategy

The framework uses a **progressive fallback approach** for encoding detection:

1. **Primary Encodings** (most common for Brazilian data):
   - UTF-8
   - Latin-1 (ISO-8859-1)
   - Windows-1252 (CP1252)

2. **Fallback Strategies**:
   - Skip malformed lines (`on_bad_lines='skip'`)
   - Use Python engine for flexibility
   - Auto-detect separators
   - Character replacement for corrupted data

3. **Last Resort**:
   - Line-by-line file reconstruction
   - Field count analysis and normalization
   - Padding/truncation for consistent structure

### Text Cleaning Implementation

#### Character-Level Cleaning
```python
# Common UTF-8 → Latin-1 artifacts
'Ã¡' → 'á'  # á read incorrectly
'Ã§' → 'ç'  # ç read incorrectly
'Ã£' → 'ã'  # ã read incorrectly
```

#### Word-Level Cleaning
```python
# Complete word pattern recognition
'institui��o' → 'instituição'
'descri��o' → 'descrição'
'munic�pio' → 'município'
```

#### Column Name Normalization
```python
# ASCII-safe column names
'Município_Instituição' → 'municipio_instituicao'
'Preço_Unitário' → 'preco_unitario'
```

### Analysis Depth

#### Schema Analysis
- **Column Inventory**: Complete list with data types
- **Naming Patterns**: Case analysis, separator usage, special characters
- **Memory Usage**: Storage optimization insights
- **Structural Issues**: Duplicate columns, naming conflicts

#### Data Quality Metrics
- **Completeness**: Missing value percentages by column
- **Consistency**: Duplicate row detection
- **Validity**: Data type validation, encoding issues
- **Accuracy**: Brazilian identifier format validation

#### Brazilian-Specific Analysis
- **CNPJ Validation**: Format checking (XX.XXX.XXX/XXXX-XX vs XXXXXXXXXXXXXXXX)
- **CNES Codes**: Healthcare facility identifier validation
- **IBGE Codes**: Municipal code format verification
- **Geographic Bounds**: Coordinate validation for Brazilian territory
- **Currency Patterns**: Real (R$) format detection

---

## Brazilian-Specific Features

### Identifier Validation

#### CNPJ (Cadastro Nacional da Pessoa Jurídica)
- **Formatted**: `12.345.678/0001-90`
- **Numeric**: `12345678000190`
- **Validation**: Format compliance and digit verification

#### CNES (Cadastro Nacional de Estabelecimentos de Saúde)
- **Format**: 7-digit numeric code
- **Purpose**: Healthcare facility identification
- **Validation**: Length and numeric format verification

#### IBGE Codes (Instituto Brasileiro de Geografia e Estatística)
- **Municipal Codes**: 6 or 7-digit codes
- **State Integration**: Cross-reference with UF codes
- **Validation**: Official code list compliance

### Geographic Data

#### Coordinate Validation
- **Latitude Range**: -35° to 6° (Brazilian territory bounds)
- **Longitude Range**: -75° to -30° (Brazilian territory bounds)
- **Precision Analysis**: Decimal place accuracy assessment

#### Administrative Divisions
- **States (UF)**: 2-letter state codes
- **Municipalities**: Name standardization and code mapping
- **Regional Classification**: Macro-region identification

### Currency Handling

#### Brazilian Real (R$) Patterns
- **Symbol Detection**: R$ prefix identification
- **Decimal Format**: Comma as decimal separator (1.234,56)
- **Thousands Separator**: Dot as thousands separator
- **Normalization**: Conversion to standard numeric format

---

## Troubleshooting

### Common Issues and Solutions

#### Encoding Problems
**Issue**: Characters display as `Ã§`, `��`, or question marks
**Solution**: 
```python
# Automatic encoding detection and cleaning
df = _load_csv_with_encoding(csv_path)
df_clean = _clean_encoding_artifacts(df)
```

#### Structural Inconsistencies
**Issue**: "Expected X fields, saw Y" errors
**Solution**:
```python
# Use robust loading with error handling
df = pd.read_csv(csv_path, on_bad_lines='skip', engine='python')
```

#### Single Column Detection
**Issue**: Files showing only 1 column when they should have more
**Solution**:
```python
# Use diagnosis to detect proper separator
diagnosis = diagnose_csv_structure(csv_path)
df = pd.read_csv(csv_path, sep=diagnosis['separator'])
```

#### Memory Issues
**Issue**: Large files causing memory problems
**Solution**:
```python
# Process in chunks for large files
for chunk in pd.read_csv(csv_path, chunksize=10000):
    process_chunk(chunk)
```

### Error Handling Patterns

The framework implements **graceful degradation**:
1. **Try optimal approach** first
2. **Fallback to safer methods** if issues occur
3. **Skip problematic data** rather than failing completely
4. **Log all issues** for transparency
5. **Provide recommendations** for manual intervention

---

## Example Outputs

### Individual File Report Structure

```
================================================================================
CSV EXPLORATION REPORT: 2020.CSV
================================================================================
Generated on: 2025-06-26 23:37:06
Analysis target: 2020.csv

1. SCHEMA ANALYSIS
--------------------------------------------------
File: 2020.csv
Shape: 71,227 rows × 20 columns
Memory Usage: 45.67 MB

COLUMNS:
   1. ano                          (int64)
   2. anvisa                       (object)
   3. cnpj_fabricante             (object)
   4. cnpj_fornecedor             (object)
   5. cnpj_instituicao            (object)
   ...

COLUMN NAMING PATTERNS:
  - Case types: {'lowercase': 18, 'uppercase': 2, 'mixed_case': 0}
  - Separators: {'underscore': 15, 'dash': 0, 'space': 0, 'camelCase': 0}

2. DATA QUALITY ASSESSMENT
--------------------------------------------------
Duplicate rows: 1,247 (1.75%)

MISSING VALUES BY COLUMN:
  anvisa                          15,623 (21.93%)
  cnpj_fabricante                  8,891 (12.48%)
  nome_instituicao                    45 (0.06%)
  ...

3. CONTENT PATTERNS ANALYSIS
--------------------------------------------------
CATEGORICAL COLUMN CANDIDATES:
  Boolean-like (2 values):
    - generico
  Low cardinality (<50 values):
    - uf (27 values)
    - tipo_compra (8 values)

GEOGRAPHIC COLUMNS:
  - uf
  - municipio_instituicao

FINANCIAL COLUMNS:
  - preco_unitario

4. BRAZILIAN-SPECIFIC PATTERNS
--------------------------------------------------
IDENTIFIER COLUMNS:
  CNPJ Patterns:
    cnpj_fabricante:
      - formatted_count: 45,231
      - numeric_only: 17,105
      - invalid_format: 8,891

5. SAMPLE DATA
--------------------------------------------------
First 3 rows showing actual data structure...

6. STANDARDIZATION RECOMMENDATIONS
--------------------------------------------------
🔧 Remove 1,247 duplicate rows
🇧🇷 Standardize CNPJ format (consider numeric-only storage)
✅ Convert boolean-like columns to proper boolean type: generico
📊 Validate geographic coordinates are within Brazilian bounds
```

### Cross-File Comparison Report

```
================================================================================
CROSS-FILE COMPARISON REPORT
================================================================================
Files analyzed: 5

FILES INCLUDED:
  1. 2020.csv (71,227 rows × 20 columns)
  2. 2021.csv (70,893 rows × 20 columns)
  3. 2022.csv (69,028 rows × 20 columns)
  4. 2023.csv (32,671 rows × 20 columns)
  5. 2024.csv (21,586 rows × 20 columns)

1. COLUMN CONSISTENCY ANALYSIS
--------------------------------------------------
Total unique columns across all files: 20
Common columns (present in all files): 20

COMMON COLUMNS:
  - ano
  - anvisa
  - cnpj_fabricante
  - cnpj_fornecedor
  - cnpj_instituicao
  - compra
  - codigo_br
  - descricao_catmat
  - fabricante
  - fornecedor
  - generico
  - insercao
  - modalidade_da_compra
  - municipio_instituicao
  - nome_instituicao
  - preco_unitario
  - qtd_itens_comprados
  - tipo_compra
  - uf
  - unidade_de_fornecimento

2. SCHEMA EVOLUTION ANALYSIS
--------------------------------------------------
COLUMN COUNT PROGRESSION:
  2020.csv: 20 columns
  2021.csv: 20 columns
  2022.csv: 20 columns
  2023.csv: 20 columns
  2024.csv: 20 columns

DATA TYPE CONSISTENCY FOR COMMON COLUMNS:
✅ All common columns have consistent data types

3. STANDARDIZATION RECOMMENDATIONS
--------------------------------------------------
📊 Design unified table schema for database implementation
🔍 Investigate data volume decrease in recent years
🇧🇷 Implement Brazilian identifier validation across all files
```

---

## Conclusion

This CSV exploration framework provides a **robust, Brazilian-focused solution** for analyzing government health data. Key achievements:

### Technical Accomplishments
- **Robust Data Loading**: Handles real-world CSV irregularities
- **Intelligent Encoding**: Automatic detection and cleaning of Portuguese text
- **Comprehensive Analysis**: Multi-dimensional data assessment
- **Automated Documentation**: Structured report generation

### Business Value
- **Data Quality Insights**: Identifies issues before database design
- **Standardization Roadmap**: Clear recommendations for schema unification
- **Brazilian Compliance**: Specialized handling for local data patterns
- **Scalable Framework**: Reusable for other government datasets

### Next Steps
1. **Database Schema Design**: Use findings to create unified table structure
2. **ETL Pipeline Development**: Implement transformation logic based on analysis
3. **Data Quality Monitoring**: Establish ongoing quality assessment
4. **Visualization Dashboard**: Create interactive exploration interface

This framework serves as the **foundation for the Arcade Brazilian Health Economics Data Pipeline**, ensuring high-quality data ingestion and providing the insights needed for effective database design and ETL implementation.

---

*Generated as part of the Arcade - Brazilian Health Economics Data Pipeline project*  
*Framework developed: June 2025*  
*Documentation version: 1.0*