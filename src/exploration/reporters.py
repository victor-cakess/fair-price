"""
Report generation module for Fair-Price Brazilian Health Data Pipeline

Generates comprehensive text reports from exploration analysis results,
converting your excellent exploration insights into professional reports
saved to the reports/exploration directory.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

# Import our configuration and logging
from config.settings import get_config
from utils.logger import get_exploration_logger, log_data_operation


class ExplorationReportGenerator:
    """
    Professional report generator for exploration analysis results
    
    Converts analysis dictionaries into formatted text reports with:
    - Executive summaries
    - Detailed findings by category
    - Cross-file comparisons
    - Actionable recommendations
    """
    
    def __init__(self):
        self.config = get_config()
        self.logger = get_exploration_logger()
        self.reports_dir = self.config.exploration_reports_dir
        
        # Ensure reports directory exists
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"📊 Exploration Report Generator initialized")
        self.logger.info(f"📁 Reports directory: {self.reports_dir}")
    
    @log_data_operation(get_exploration_logger(), "Individual file report generation")
    def generate_file_report(self, analysis: Dict[str, Any], output_filename: Optional[str] = None) -> Path:
        """
        Generate a comprehensive text report for a single file analysis
        
        Args:
            analysis: Analysis dictionary from BrazilianHealthDataAnalyzer
            output_filename: Optional custom filename
            
        Returns:
            Path to generated report file
        """
        filename = analysis['schema']['filename']
        
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = Path(filename).stem
            output_filename = f"exploration_report_{base_name}_{timestamp}.txt"
        
        output_path = self.reports_dir / output_filename
        
        # Generate report content
        report_lines = self._generate_file_report_content(analysis)
        
        # Write to file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        self.logger.info(f"📄 Individual report saved: {output_path.name}")
        return output_path
    
    @log_data_operation(get_exploration_logger(), "Cross-file comparison report generation")
    def generate_comparison_report(self, analyses: List[Dict[str, Any]], output_filename: Optional[str] = None) -> Path:
        """
        Generate a cross-file comparison report for multiple analyses
        
        Args:
            analyses: List of analysis dictionaries
            output_filename: Optional custom filename
            
        Returns:
            Path to generated comparison report
        """
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"comparison_report_{timestamp}.txt"
        
        output_path = self.reports_dir / output_filename
        
        # Generate comparison content
        report_lines = self._generate_comparison_report_content(analyses)
        
        # Write to file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        self.logger.info(f"📊 Comparison report saved: {output_path.name}")
        return output_path
    
    def _generate_file_report_content(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate content lines for individual file report"""
        schema = analysis['schema']
        quality = analysis['quality']
        content = analysis['content']
        brazilian = analysis['brazilian_specifics']
        
        lines = []
        
        # Header
        lines.extend([
            "=" * 80,
            f"FAIR-PRICE EXPLORATION REPORT: {schema['filename'].upper()}",
            "=" * 80,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Target File: {schema['filename']}",
            "",
        ])
        
        # Executive Summary
        lines.extend(self._format_executive_summary(analysis))
        
        # Schema Analysis
        lines.extend(self._format_schema_section(schema))
        
        # Data Quality Analysis
        lines.extend(self._format_quality_section(quality))
        
        # Content Patterns
        lines.extend(self._format_content_section(content))
        
        # Brazilian-Specific Analysis
        lines.extend(self._format_brazilian_section(brazilian))
        
        # Sample Data
        lines.extend(self._format_sample_section(analysis['sample_data']))
        
        # Recommendations
        lines.extend(self._format_recommendations_section(analysis))
        
        # Footer
        lines.extend([
            "",
            "=" * 80,
            "END OF EXPLORATION REPORT",
            "=" * 80
        ])
        
        return lines
    
    def _format_executive_summary(self, analysis: Dict[str, Any]) -> List[str]:
        """Format executive summary section"""
        schema = analysis['schema']
        quality = analysis['quality']
        
        # Calculate key metrics
        total_cells = schema['row_count'] * schema['column_count']
        missing_cells = sum(quality['missing_values'].values())
        completeness = (1 - missing_cells / total_cells) * 100
        
        lines = [
            "📋 EXECUTIVE SUMMARY",
            "-" * 50,
            f"File: {schema['filename']}",
            f"Dataset Size: {schema['row_count']:,} rows × {schema['column_count']} columns",
            f"Memory Usage: {schema['memory_usage'] / 1024 / 1024:.2f} MB",
            f"Data Completeness: {completeness:.1f}%",
            f"Duplicate Records: {quality['duplicate_rows']:,} ({quality['duplicate_percentage']:.2f}%)",
            "",
            "🎯 KEY FINDINGS:",
        ]
        
        # Add key findings
        brazilian = analysis['brazilian_specifics']
        content = analysis['content']
        
        findings = []
        
        if brazilian['cnpj_patterns']:
            findings.append(f"• {len(brazilian['cnpj_patterns'])} CNPJ identifier columns detected")
        
        if brazilian['state_references']:
            findings.append(f"• {len(brazilian['state_references'])} geographic state columns found")
        
        if content['categorical_candidates']['boolean_like']:
            findings.append(f"• {len(content['categorical_candidates']['boolean_like'])} boolean-like columns identified")
        
        if quality['duplicate_percentage'] > 10:
            findings.append(f"• High duplicate rate ({quality['duplicate_percentage']:.1f}%) requires attention")
        
        if completeness < 90:
            findings.append(f"• Data completeness below 90% - review missing values")
        
        if not findings:
            findings.append("• Dataset appears well-structured with no major issues")
        
        lines.extend([f"  {finding}" for finding in findings])
        lines.append("")
        
        return lines
    
    def _format_schema_section(self, schema: Dict[str, Any]) -> List[str]:
        """Format schema analysis section"""
        lines = [
            "1. SCHEMA ANALYSIS",
            "-" * 50,
            f"Dataset Dimensions: {schema['shape'][0]:,} rows × {schema['shape'][1]} columns",
            f"Memory Footprint: {schema['memory_usage'] / 1024 / 1024:.2f} MB",
            "",
            "COLUMN INVENTORY:",
        ]
        
        # List all columns with data types
        for i, col in enumerate(schema['columns'], 1):
            dtype = str(schema['dtypes'].get(col, 'unknown'))
            lines.append(f"  {i:2d}. {col:<40} ({dtype})")
        
        lines.extend([
            "",
            "COLUMN NAMING PATTERNS:",
        ])
        
        patterns = schema['column_name_patterns']
        lines.extend([
            f"  Case Distribution: {patterns['case_types']}",
            f"  Separator Usage: {patterns['separators']}",
            f"  Special Characters: {patterns['special_chars']} columns",
            f"  Numeric Prefixes: {patterns['numeric_prefixes']} columns",
            ""
        ])
        
        if schema['duplicated_columns']:
            lines.extend([
                "⚠️  DUPLICATE COLUMN NAMES DETECTED:",
                *[f"  - {col}" for col in schema['duplicated_columns']],
                ""
            ])
        
        return lines
    
    def _format_quality_section(self, quality: Dict[str, Any]) -> List[str]:
        """Format data quality section"""
        lines = [
            "2. DATA QUALITY ASSESSMENT",
            "-" * 50,
            f"Duplicate Records: {quality['duplicate_rows']:,} ({quality['duplicate_percentage']:.2f}%)",
            "",
            "MISSING VALUES ANALYSIS:",
        ]
        
        # Sort columns by missing percentage
        missing_sorted = sorted(quality['missing_percentage'].items(), key=lambda x: x[1], reverse=True)
        
        # Show top missing columns
        top_missing = [(col, pct) for col, pct in missing_sorted if pct > 0][:10]
        
        if top_missing:
            for col, pct in top_missing:
                count = quality['missing_values'][col]
                lines.append(f"  {col:<40} {count:>8,} ({pct:>6.2f}%)")
        else:
            lines.append("  No missing values detected")
        
        lines.extend(["", "DATA INTEGRITY ISSUES:"])
        
        # Empty strings
        empty_strings = quality['empty_strings']
        empty_total = sum(empty_strings.values()) if empty_strings else 0
        if empty_total > 0:
            lines.append(f"  Empty Strings: {empty_total:,} occurrences")
            top_empty = sorted(empty_strings.items(), key=lambda x: x[1], reverse=True)[:5]
            for col, count in top_empty:
                if count > 0:
                    lines.append(f"    {col}: {count:,}")
        else:
            lines.append("  Empty Strings: None detected")
        
        # Encoding issues
        encoding_issues = quality['encoding_issues']
        encoding_total = sum(encoding_issues.values()) if encoding_issues else 0
        if encoding_total > 0:
            lines.append(f"  Encoding Artifacts: {encoding_total:,} occurrences")
        else:
            lines.append("  Encoding Artifacts: None detected")
        
        # Numeric columns with text
        numeric_text = quality['numeric_columns_with_text']
        if numeric_text:
            lines.append("  Numeric Columns with Text:")
            for col, count in numeric_text.items():
                lines.append(f"    {col}: {count:,} non-numeric values")
        else:
            lines.append("  Numeric Data: Clean")
        
        lines.append("")
        return lines
    
    def _format_content_section(self, content: Dict[str, Any]) -> List[str]:
        """Format content patterns section"""
        lines = [
            "3. CONTENT PATTERNS ANALYSIS",
            "-" * 50,
            "CARDINALITY ANALYSIS:",
        ]
        
        # Unique value counts (top and bottom)
        unique_counts = content['unique_value_counts']
        sorted_counts = sorted(unique_counts.items(), key=lambda x: x[1])
        
        lines.append("  Low Cardinality (≤10 unique values):")
        low_card = [(col, count) for col, count in sorted_counts if count <= 10]
        if low_card:
            for col, count in low_card:
                lines.append(f"    {col}: {count} unique values")
        else:
            lines.append("    None detected")
        
        lines.append("  High Cardinality (top 5):")
        high_card = sorted_counts[-5:]
        for col, count in reversed(high_card):
            lines.append(f"    {col}: {count:,} unique values")
        
        lines.extend(["", "CATEGORICAL CANDIDATES:"])
        
        categorical = content['categorical_candidates']
        if categorical['boolean_like']:
            lines.append("  Boolean-like (2 values):")
            for col in categorical['boolean_like']:
                lines.append(f"    - {col}")
        
        if categorical['low_cardinality']:
            lines.append("  Low Cardinality (<50 values):")
            for col in categorical['low_cardinality'][:10]:  # Limit to 10
                count = unique_counts[col]
                lines.append(f"    - {col} ({count} values)")
        
        lines.extend(["", "DOMAIN-SPECIFIC COLUMNS:"])
        
        if content['geographic_columns']:
            lines.append("  Geographic:")
            for col in content['geographic_columns']:
                lines.append(f"    - {col}")
        
        if content['financial_columns']:
            lines.append("  Financial:")
            for col in content['financial_columns']:
                lines.append(f"    - {col}")
        
        date_cols = content['date_columns']
        if date_cols['potential_date_columns'] or date_cols['datetime_columns']:
            lines.append("  Date/Time:")
            for col in date_cols['potential_date_columns']:
                lines.append(f"    - {col} (potential)")
            for col in date_cols['datetime_columns']:
                lines.append(f"    - {col} (confirmed)")
        
        lines.append("")
        return lines
    
    def _format_brazilian_section(self, brazilian: Dict[str, Any]) -> List[str]:
        """Format Brazilian-specific analysis section"""
        lines = [
            "4. BRAZILIAN-SPECIFIC PATTERNS",
            "-" * 50,
            "IDENTIFIER SYSTEMS:",
        ]
        
        # CNPJ Analysis
        cnpj_patterns = brazilian['cnpj_patterns']
        if cnpj_patterns:
            lines.append("  CNPJ (Corporate Tax ID) Columns:")
            for col, patterns in cnpj_patterns.items():
                lines.append(f"    {col}:")
                for pattern_type, count in patterns.items():
                    lines.append(f"      - {pattern_type}: {count:,}")
        else:
            lines.append("  CNPJ Columns: None detected")
        
        # CNES Analysis
        cnes_patterns = brazilian['cnes_patterns']
        if cnes_patterns:
            lines.append("  CNES (Health Facility ID) Columns:")
            for col, patterns in cnes_patterns.items():
                lines.append(f"    {col}:")
                for pattern_type, count in patterns.items():
                    lines.append(f"      - {pattern_type}: {count:,}")
        else:
            lines.append("  CNES Columns: None detected")
        
        # IBGE Codes
        ibge_codes = brazilian['ibge_codes']
        if ibge_codes:
            lines.append("  IBGE (Municipality) Codes:")
            for col, patterns in ibge_codes.items():
                lines.append(f"    {col}:")
                for pattern_type, count in patterns.items():
                    lines.append(f"      - {pattern_type}: {count:,}")
        else:
            lines.append("  IBGE Codes: None detected")
        
        lines.extend(["", "CURRENCY PATTERNS:"])
        
        currency = brazilian['brazilian_currency']
        if currency:
            for col, patterns in currency.items():
                lines.append(f"  {col}:")
                for pattern_type, count in patterns.items():
                    lines.append(f"    - {pattern_type}: {count:,}")
        else:
            lines.append("  No Brazilian currency patterns detected")
        
        lines.extend(["", "GEOGRAPHIC REFERENCES:"])
        
        if brazilian['state_references']:
            lines.append("  State Columns:")
            for col in brazilian['state_references']:
                lines.append(f"    - {col}")
        
        if brazilian['municipality_patterns']:
            lines.append("  Municipality Columns:")
            for col in brazilian['municipality_patterns']:
                lines.append(f"    - {col}")
        
        if not brazilian['state_references'] and not brazilian['municipality_patterns']:
            lines.append("  No geographic reference columns detected")
        
        lines.append("")
        return lines
    
    def _format_sample_section(self, sample_data: Dict[str, Any]) -> List[str]:
        """Format sample data section"""
        lines = [
            "5. SAMPLE DATA PREVIEW",
            "-" * 50,
            "FIRST 3 RECORDS:",
        ]
        
        first_rows = sample_data['first_5_rows'][:3]
        for i, row in enumerate(first_rows, 1):
            lines.append(f"  Record {i}:")
            for col, value in row.items():
                # Truncate long values
                str_value = str(value)
                if len(str_value) > 60:
                    str_value = str_value[:57] + "..."
                lines.append(f"    {col}: {str_value}")
            lines.append("")
        
        return lines
    
    def _format_recommendations_section(self, analysis: Dict[str, Any]) -> List[str]:
        """Format recommendations section"""
        lines = [
            "6. RECOMMENDATIONS",
            "-" * 50,
        ]
        
        recommendations = self._generate_recommendations(analysis)
        
        if recommendations:
            lines.extend(recommendations)
        else:
            lines.append("✅ Dataset appears well-structured with no major issues detected")
        
        lines.append("")
        return lines
    
    def _generate_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on analysis"""
        recommendations = []
        
        schema = analysis['schema']
        quality = analysis['quality']
        content = analysis['content']
        brazilian = analysis['brazilian_specifics']
        
        # Schema recommendations
        patterns = schema['column_name_patterns']
        if patterns['case_types']['mixed_case'] > 0:
            recommendations.append("🔧 SCHEMA: Standardize column naming case (recommend snake_case)")
        
        if patterns['separators']['space'] > 0:
            recommendations.append("🔧 SCHEMA: Replace spaces in column names with underscores")
        
        if patterns['special_chars'] > 0:
            recommendations.append("🔧 SCHEMA: Remove special characters from column names")
        
        # Quality recommendations
        if quality['duplicate_percentage'] > 5:
            recommendations.append(f"🧹 QUALITY: Remove {quality['duplicate_rows']:,} duplicate rows ({quality['duplicate_percentage']:.1f}%)")
        
        high_missing = [col for col, pct in quality['missing_percentage'].items() if pct > 50]
        if high_missing:
            recommendations.append(f"⚠️  QUALITY: Review columns with >50% missing data: {', '.join(high_missing[:3])}")
        
        if quality['numeric_columns_with_text']:
            recommendations.append("🔧 QUALITY: Clean numeric columns containing text values")
        
        # Brazilian-specific recommendations
        if brazilian['cnpj_patterns']:
            recommendations.append("🇧🇷 BRAZILIAN: Standardize CNPJ format (consider validation and formatting)")
        
        if brazilian['cnes_patterns']:
            recommendations.append("🇧🇷 BRAZILIAN: Validate CNES health facility codes")
        
        # Content recommendations
        boolean_cols = content['categorical_candidates']['boolean_like']
        if boolean_cols:
            recommendations.append(f"✅ CONTENT: Convert boolean-like columns to proper boolean type: {', '.join(boolean_cols[:3])}")
        
        return recommendations
    
    def _generate_comparison_report_content(self, analyses: List[Dict[str, Any]]) -> List[str]:
        """Generate cross-file comparison report content"""
        lines = [
            "=" * 80,
            "FAIR-PRICE CROSS-FILE COMPARISON REPORT",
            "=" * 80,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Files Analyzed: {len(analyses)}",
            "",
        ]
        
        # Files summary
        lines.append("FILES INCLUDED:")
        for i, analysis in enumerate(analyses, 1):
            schema = analysis['schema']
            lines.append(f"  {i}. {schema['filename']} ({schema['shape'][0]:,} rows × {schema['shape'][1]} columns)")
        
        lines.extend(["", "=" * 60, ""])
        
        # Column consistency analysis
        lines.extend(self._compare_column_consistency(analyses))
        
        # Data quality comparison
        lines.extend(self._compare_data_quality(analyses))
        
        # Brazilian patterns comparison
        lines.extend(self._compare_brazilian_patterns(analyses))
        
        # Overall recommendations
        lines.extend(self._compare_recommendations(analyses))
        
        return lines
    
    def _compare_column_consistency(self, analyses: List[Dict[str, Any]]) -> List[str]:
        """Compare column consistency across files"""
        lines = [
            "1. COLUMN CONSISTENCY ANALYSIS",
            "-" * 50,
        ]
        
        # Get all unique columns
        all_columns = set()
        file_columns = {}
        
        for analysis in analyses:
            filename = analysis['schema']['filename']
            columns = set(analysis['schema']['columns'])
            file_columns[filename] = columns
            all_columns.update(columns)
        
        # Find common columns
        common_columns = set.intersection(*file_columns.values()) if file_columns else set()
        
        lines.extend([
            f"Total Unique Columns: {len(all_columns)}",
            f"Common Columns: {len(common_columns)}",
            f"Consistency Rate: {len(common_columns)/len(all_columns)*100:.1f}%",
            "",
            "COMMON COLUMNS:",
        ])
        
        for col in sorted(common_columns):
            lines.append(f"  - {col}")
        
        lines.extend(["", "FILE-SPECIFIC COLUMNS:"])
        for filename, cols in file_columns.items():
            specific = cols - common_columns
            if specific:
                lines.append(f"  {filename}:")
                for col in sorted(specific):
                    lines.append(f"    - {col}")
        
        lines.append("")
        return lines
    
    def _compare_data_quality(self, analyses: List[Dict[str, Any]]) -> List[str]:
        """Compare data quality across files"""
        lines = [
            "2. DATA QUALITY COMPARISON",
            "-" * 50,
        ]
        
        for analysis in analyses:
            schema = analysis['schema']
            quality = analysis['quality']
            
            total_cells = schema['row_count'] * schema['column_count']
            missing_cells = sum(quality['missing_values'].values())
            completeness = (1 - missing_cells / total_cells) * 100
            
            lines.append(f"{schema['filename']}:")
            lines.append(f"  Completeness: {completeness:.1f}%")
            lines.append(f"  Duplicates: {quality['duplicate_percentage']:.2f}%")
            lines.append("")
        
        return lines
    
    def _compare_brazilian_patterns(self, analyses: List[Dict[str, Any]]) -> List[str]:
        """Compare Brazilian patterns across files"""
        lines = [
            "3. BRAZILIAN PATTERNS COMPARISON",
            "-" * 50,
        ]
        
        for analysis in analyses:
            schema = analysis['schema']
            brazilian = analysis['brazilian_specifics']
            
            lines.append(f"{schema['filename']}:")
            lines.append(f"  CNPJ Columns: {len(brazilian['cnpj_patterns'])}")
            lines.append(f"  CNES Columns: {len(brazilian['cnes_patterns'])}")
            lines.append(f"  State References: {len(brazilian['state_references'])}")
            lines.append("")
        
        return lines
    
    def _compare_recommendations(self, analyses: List[Dict[str, Any]]) -> List[str]:
        """Generate comparison-based recommendations"""
        lines = [
            "4. UNIFIED RECOMMENDATIONS",
            "-" * 50,
            "🗂️  Implement unified column schema across all files",
            "🔧 Standardize Brazilian identifier formats (CNPJ, CNES)",
            "📊 Create master data quality monitoring dashboard",
            "🇧🇷 Validate geographic and institutional references",
            ""
        ]
        
        return lines


# Convenience functions
def generate_exploration_reports(analyses: List[Dict[str, Any]]) -> Dict[str, Path]:
    """
    Generate both individual and comparison reports
    
    Args:
        analyses: List of analysis dictionaries
        
    Returns:
        Dictionary with report types and their file paths
    """
    generator = ExplorationReportGenerator()
    reports = {}
    
    # Generate individual reports
    for analysis in analyses:
        report_path = generator.generate_file_report(analysis)
        reports[f"individual_{analysis['schema']['filename']}"] = report_path
    
    # Generate comparison report
    if len(analyses) > 1:
        comparison_path = generator.generate_comparison_report(analyses)
        reports['comparison'] = comparison_path
    
    return reports


# Example usage
if __name__ == "__main__":
    print("📊 Fair-Price Exploration Report Generator")
    print("=" * 60)
    
    # This would typically be called after running exploration analysis
    print("💡 This module generates reports from exploration analysis results")
    print("📁 Reports are saved to: reports/exploration/")
    print("🔧 Use generate_exploration_reports() with analysis results")