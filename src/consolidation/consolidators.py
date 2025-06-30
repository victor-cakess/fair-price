"""
Data consolidation module for Fair-Price Brazilian Health Data Pipeline.

Merges multiple standardized CSV files into a single, consolidated dataset
with intelligent duplicate handling, data validation, and quality reporting.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
import time
from datetime import datetime


class HealthDataConsolidator:
    """
    Consolidates multiple standardized health procurement CSV files into a unified dataset.
    
    Features:
    - Intelligent duplicate detection and resolution
    - Cross-year data validation
    - Quality scoring and reporting
    - Multiple output formats (CSV, Parquet)
    - Data lineage tracking
    """
    
    def __init__(self, config, logger):
        """Initialize consolidator with config and logger."""
        self.config = config
        self.logger = logger
        self.consolidation_stats = {}
    
    def load_standardized_files(self, input_dir: Path) -> Dict[int, pd.DataFrame]:
        """
        Load all standardized CSV files from the processed directory.
        
        Args:
            input_dir: Directory containing standardized CSV files
            
        Returns:
            Dictionary mapping year to DataFrame
        """
        self.logger.info(f"📂 Loading standardized files from {input_dir}")
        
        csv_files = list(input_dir.glob("*.csv"))
        if not csv_files:
            raise ValueError(f"No CSV files found in {input_dir}")
        
        dataframes = {}
        total_rows = 0
        
        for csv_file in sorted(csv_files):
            try:
                # Extract year from filename
                year = int(csv_file.stem)
                
                # Load DataFrame
                df = pd.read_csv(csv_file, encoding='utf-8')
                dataframes[year] = df
                total_rows += len(df)
                
                self.logger.info(f"   ✅ {year}: {len(df):,} rows, {len(df.columns)} columns")
                
            except Exception as e:
                self.logger.error(f"   ❌ Failed to load {csv_file.name}: {str(e)}")
        
        self.logger.info(f"📊 Total loaded: {len(dataframes)} files, {total_rows:,} rows")
        return dataframes
    
    def validate_schema_consistency(self, dataframes: Dict[int, pd.DataFrame]) -> Dict[str, Any]:
        """
        Validate that all DataFrames have consistent schemas.
        
        Args:
            dataframes: Dictionary of year -> DataFrame
            
        Returns:
            Schema validation report
        """
        self.logger.info("🔍 Validating schema consistency across files...")
        
        if not dataframes:
            return {'error': 'No dataframes to validate'}
        
        # Get all unique columns across all files
        all_columns = set()
        file_columns = {}
        
        for year, df in dataframes.items():
            columns = set(df.columns)
            file_columns[year] = columns
            all_columns.update(columns)
        
        # Find common columns
        common_columns = set.intersection(*file_columns.values()) if file_columns else set()
        
        # Schema validation report
        validation_report = {
            'total_unique_columns': len(all_columns),
            'common_columns': len(common_columns),
            'common_column_list': sorted(list(common_columns)),
            'consistency_rate': len(common_columns) / len(all_columns) * 100 if all_columns else 0,
            'file_specific_columns': {},
            'missing_columns_by_file': {}
        }
        
        # Find file-specific columns
        for year, columns in file_columns.items():
            specific = columns - common_columns
            missing = common_columns - columns
            
            if specific:
                validation_report['file_specific_columns'][year] = sorted(list(specific))
            if missing:
                validation_report['missing_columns_by_file'][year] = sorted(list(missing))
        
        # Log findings
        self.logger.info(f"   📊 Schema consistency: {validation_report['consistency_rate']:.1f}%")
        self.logger.info(f"   📋 Common columns: {len(common_columns)}/{len(all_columns)}")
        
        if validation_report['file_specific_columns']:
            self.logger.warning("   ⚠️  Files have inconsistent columns")
        else:
            self.logger.info("   ✅ All files have consistent schemas")
        
        return validation_report
    
    def standardize_schemas(self, dataframes: Dict[int, pd.DataFrame]) -> Dict[int, pd.DataFrame]:
        """
        Standardize schemas across all DataFrames to ensure consistency.
        
        Args:
            dataframes: Dictionary of year -> DataFrame
            
        Returns:
            Dictionary with standardized DataFrames
        """
        self.logger.info("🔧 Standardizing schemas across all files...")
        
        # Get the union of all columns
        all_columns = set()
        for df in dataframes.values():
            all_columns.update(df.columns)
        
        # Use unified schema from config if available, otherwise use discovered columns
        if hasattr(self.config, 'unified_schema'):
            target_columns = self.config.unified_schema
            self.logger.info(f"   📋 Using config unified schema: {len(target_columns)} columns")
        else:
            target_columns = sorted(list(all_columns))
            self.logger.info(f"   📋 Using discovered schema: {len(target_columns)} columns")
        
        standardized_dfs = {}
        
        for year, df in dataframes.items():
            df_std = df.copy()
            
            # Add missing columns with None values
            for col in target_columns:
                if col not in df_std.columns:
                    df_std[col] = None
                    self.logger.debug(f"   📝 Added missing column '{col}' to {year}")
            
            # Reorder columns to match target schema
            df_std = df_std[target_columns]
            
            standardized_dfs[year] = df_std
            self.logger.info(f"   ✅ {year}: standardized to {len(df_std.columns)} columns")
        
        return standardized_dfs
    
    def detect_duplicates_across_years(self, dataframes: Dict[int, pd.DataFrame]) -> Dict[str, Any]:
        """
        Detect potential duplicates across different years.
        
        Args:
            dataframes: Dictionary of year -> DataFrame
            
        Returns:
            Duplicate detection report
        """
        self.logger.info("🔍 Detecting cross-year duplicates...")
        
        # Combine all data for duplicate detection
        combined_data = []
        for year, df in dataframes.items():
            df_with_source = df.copy()
            df_with_source['_source_year'] = year
            combined_data.append(df_with_source)
        
        if not combined_data:
            return {'total_duplicates': 0, 'duplicate_details': {}}
        
        combined_df = pd.concat(combined_data, ignore_index=True)
        
        # Define key columns for duplicate detection (excluding year and source)
        key_columns = [col for col in combined_df.columns 
                      if col not in ['ano', '_source_year'] and not col.startswith('_')]
        
        # Find duplicates based on key columns
        duplicate_mask = combined_df.duplicated(subset=key_columns, keep=False)
        duplicates = combined_df[duplicate_mask]
        
        duplicate_report = {
            'total_records': len(combined_df),
            'total_duplicates': len(duplicates),
            'duplicate_percentage': len(duplicates) / len(combined_df) * 100,
            'unique_duplicate_groups': 0,
            'cross_year_duplicates': 0,
            'same_year_duplicates': 0
        }
        
        if len(duplicates) > 0:
            # Analyze duplicate groups
            duplicate_groups = duplicates.groupby(key_columns)
            duplicate_report['unique_duplicate_groups'] = len(duplicate_groups)
            
            # Count cross-year vs same-year duplicates
            for name, group in duplicate_groups:
                unique_years = group['_source_year'].nunique()
                if unique_years > 1:
                    duplicate_report['cross_year_duplicates'] += len(group)
                else:
                    duplicate_report['same_year_duplicates'] += len(group)
        
        self.logger.info(f"   📊 Total duplicates: {duplicate_report['total_duplicates']:,} ({duplicate_report['duplicate_percentage']:.1f}%)")
        self.logger.info(f"   🔀 Cross-year duplicates: {duplicate_report['cross_year_duplicates']:,}")
        self.logger.info(f"   📅 Same-year duplicates: {duplicate_report['same_year_duplicates']:,}")
        
        return duplicate_report
    
    def resolve_duplicates(self, dataframes: Dict[int, pd.DataFrame], 
                         strategy: str = "keep_latest") -> pd.DataFrame:
        """
        Resolve duplicates and merge all DataFrames into a single consolidated dataset.
        
        Args:
            dataframes: Dictionary of year -> DataFrame
            strategy: Duplicate resolution strategy ('keep_latest', 'keep_all', 'aggregate')
            
        Returns:
            Consolidated DataFrame with duplicates resolved
        """
        self.logger.info(f"🔧 Consolidating data with '{strategy}' duplicate strategy...")
        
        # Add source tracking
        all_dataframes = []
        for year, df in dataframes.items():
            df_with_source = df.copy()
            df_with_source['_source_year'] = year
            df_with_source['_consolidation_id'] = range(len(df))
            all_dataframes.append(df_with_source)
        
        # Combine all data
        consolidated_df = pd.concat(all_dataframes, ignore_index=True)
        original_count = len(consolidated_df)
        
        if strategy == "keep_latest":
            # Keep the record from the latest year for each duplicate group
            key_columns = [col for col in consolidated_df.columns 
                          if col not in ['ano', '_source_year', '_consolidation_id']]
            
            # Sort by source year (latest first) and drop duplicates
            consolidated_df = consolidated_df.sort_values('_source_year', ascending=False)
            consolidated_df = consolidated_df.drop_duplicates(subset=key_columns, keep='first')
            
        elif strategy == "keep_all":
            # Keep all records (no deduplication)
            pass
            
        elif strategy == "aggregate":
            # Aggregate duplicates (this would need more complex logic)
            self.logger.warning("Aggregate strategy not fully implemented, using keep_latest")
            consolidated_df = consolidated_df.sort_values('_source_year', ascending=False)
            key_columns = [col for col in consolidated_df.columns 
                          if col not in ['ano', '_source_year', '_consolidation_id']]
            consolidated_df = consolidated_df.drop_duplicates(subset=key_columns, keep='first')
        
        # Remove consolidation helper columns
        business_columns = [col for col in consolidated_df.columns 
                           if not col.startswith('_')]
        final_df = consolidated_df[business_columns]
        
        final_count = len(final_df)
        reduction_percentage = (original_count - final_count) / original_count * 100
        
        self.logger.info(f"   📊 Consolidation complete:")
        self.logger.info(f"      Input records: {original_count:,}")
        self.logger.info(f"      Output records: {final_count:,}")
        self.logger.info(f"      Reduction: {reduction_percentage:.1f}%")
        
        return final_df
    
    def validate_consolidated_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate the final consolidated dataset.
        
        Args:
            df: Consolidated DataFrame
            
        Returns:
            Validation report
        """
        self.logger.info("✅ Validating consolidated dataset...")
        
        validation_report = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
            'year_distribution': df['ano'].value_counts().to_dict() if 'ano' in df.columns else {},
            'completeness_by_column': {},
            'data_types': df.dtypes.to_dict(),
            'quality_summary': {}
        }
        
        # Calculate completeness
        for col in df.columns:
            completeness = (1 - df[col].isnull().sum() / len(df)) * 100
            validation_report['completeness_by_column'][col] = completeness
        
        overall_completeness = sum(validation_report['completeness_by_column'].values()) / len(df.columns)
        validation_report['overall_completeness'] = overall_completeness
        
        # Basic quality checks
        validation_report['quality_summary'] = {
            'has_duplicates': df.duplicated().any(),
            'duplicate_count': df.duplicated().sum(),
            'has_null_values': df.isnull().any().any(),
            'null_percentage': df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100
        }
        
        self.logger.info(f"   📊 Final dataset: {validation_report['total_rows']:,} rows × {validation_report['total_columns']} columns")
        self.logger.info(f"   💾 Memory usage: {validation_report['memory_usage_mb']:.1f} MB")
        self.logger.info(f"   ✅ Overall completeness: {overall_completeness:.1f}%")
        
        if 'ano' in df.columns:
            self.logger.info("   📅 Year distribution:")
            for year, count in sorted(validation_report['year_distribution'].items()):
                self.logger.info(f"      {year}: {count:,} records")
        
        return validation_report
    
    def save_consolidated_data(self, df: pd.DataFrame, output_dir: Path, 
                             filename_prefix: str = "consolidated_health_data") -> Dict[str, Path]:
        """
        Save consolidated data in multiple formats.
        
        Args:
            df: Consolidated DataFrame
            output_dir: Output directory
            filename_prefix: Prefix for output filenames
            
        Returns:
            Dictionary mapping format to file path
        """
        self.logger.info(f"💾 Saving consolidated data to {output_dir}")
        
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        saved_files = {}
        
        # Save as CSV
        csv_path = output_dir / f"{filename_prefix}_{timestamp}.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8')
        saved_files['csv'] = csv_path
        
        # Save as Parquet (more efficient for large datasets)
        try:
            parquet_path = output_dir / f"{filename_prefix}_{timestamp}.parquet"
            df.to_parquet(parquet_path, index=False)
            saved_files['parquet'] = parquet_path
        except Exception as e:
            self.logger.warning(f"Could not save Parquet format: {str(e)}")
        
        # Log file sizes
        for format_name, file_path in saved_files.items():
            size_mb = file_path.stat().st_size / (1024 * 1024)
            self.logger.info(f"   ✅ {format_name.upper()}: {file_path.name} ({size_mb:.1f} MB)")
        
        return saved_files
    
    def generate_consolidation_report(self, 
                                    schema_validation: Dict[str, Any],
                                    duplicate_detection: Dict[str, Any],
                                    final_validation: Dict[str, Any],
                                    processing_time: float) -> Dict[str, Any]:
        """
        Generate comprehensive consolidation report.
        
        Args:
            schema_validation: Schema validation results
            duplicate_detection: Duplicate detection results
            final_validation: Final validation results
            processing_time: Total processing time
            
        Returns:
            Comprehensive consolidation report
        """
        report = {
            'consolidation_timestamp': datetime.now().isoformat(),
            'processing_time_seconds': processing_time,
            'schema_validation': schema_validation,
            'duplicate_detection': duplicate_detection,
            'final_validation': final_validation,
            'consolidation_summary': {
                'input_files': len(schema_validation.get('file_specific_columns', {})) + len([k for k in schema_validation.keys() if isinstance(k, int)]),
                'output_records': final_validation['total_rows'],
                'data_quality_score': final_validation['overall_completeness'],
                'duplicate_reduction': duplicate_detection.get('duplicate_percentage', 0),
                'schema_consistency': schema_validation.get('consistency_rate', 0)
            }
        }
        
        return report
    
    def consolidate_all_data(self, input_dir: Path, output_dir: Path, 
                           duplicate_strategy: str = "keep_latest") -> Dict[str, Any]:
        """
        Complete consolidation workflow: load, validate, merge, and save.
        
        Args:
            input_dir: Directory with standardized CSV files
            output_dir: Directory for consolidated output
            duplicate_strategy: Strategy for handling duplicates
            
        Returns:
            Complete consolidation report
        """
        start_time = time.time()
        
        self.logger.info("🚀 Starting complete data consolidation process...")
        
        # Step 1: Load standardized files
        dataframes = self.load_standardized_files(input_dir)
        
        # Step 2: Validate schema consistency
        schema_validation = self.validate_schema_consistency(dataframes)
        
        # Step 3: Standardize schemas if needed
        if schema_validation['consistency_rate'] < 100:
            dataframes = self.standardize_schemas(dataframes)
        
        # Step 4: Detect duplicates
        duplicate_detection = self.detect_duplicates_across_years(dataframes)
        
        # Step 5: Resolve duplicates and consolidate
        consolidated_df = self.resolve_duplicates(dataframes, duplicate_strategy)
        
        # Step 6: Validate final dataset
        final_validation = self.validate_consolidated_data(consolidated_df)
        
        # Step 7: Save consolidated data
        saved_files = self.save_consolidated_data(consolidated_df, output_dir)
        
        processing_time = time.time() - start_time
        
        # Step 8: Generate comprehensive report
        report = self.generate_consolidation_report(
            schema_validation, duplicate_detection, 
            final_validation, processing_time
        )
        report['saved_files'] = {k: str(v) for k, v in saved_files.items()}
        
        self.logger.info(f"✅ Consolidation completed in {processing_time:.2f} seconds")
        self.logger.info(f"📊 Final dataset: {final_validation['total_rows']:,} records")
        
        return report


# Convenience function for easy usage
def consolidate_health_data(input_dir: Path, output_dir: Path, 
                          config=None, logger=None) -> Dict[str, Any]:
    """
    Simple function to consolidate standardized health data files.
    
    Args:
        input_dir: Directory containing standardized CSV files
        output_dir: Directory for consolidated output
        config: Configuration object (optional)
        logger: Logger instance (optional)
        
    Returns:
        Consolidation report
    """
    if config is None:
        from config.settings import get_config
        config = get_config()
    
    if logger is None:
        from utils.logger import get_standardization_logger
        logger = get_standardization_logger()
    
    consolidator = HealthDataConsolidator(config, logger)
    return consolidator.consolidate_all_data(input_dir, output_dir)