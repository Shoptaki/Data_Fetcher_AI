import streamlit as st
import pandas as pd
import json
import sqlite3
import re
from datetime import datetime
from typing import Dict, List, Any, Union, Optional
from io import StringIO, BytesIO
import csv
from pathlib import Path
import zipfile

# Configure Streamlit page
st.set_page_config(
    page_title="📄 SQL to NoSQL Database Converter",
    page_icon="📄",
    layout="wide"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        text-align: center;
        padding: 2rem 0;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .conversion-step {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
        border-left: 4px solid #667eea;
    }
    .success-box {
        background: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .error-box {
        background: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
    }
    .db-tag {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.8em;
        font-weight: bold;
        margin-left: 10px;
        color: white;
    }
    .sql-tag { background: #3498db; }
    .nosql-tag { background: #e67e22; }
</style>
""", unsafe_allow_html=True)


class DatabaseConverter:
    """Main class for handling database conversions"""

    def __init__(self):
        self.conversion_log = []
        self.intermediate_json = None

    def log(self, message: str) -> None:
        """Add message to conversion log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.conversion_log.append(f"[{timestamp}] {message}")

    def get_transformation_strategy(self, source: str, target: str) -> str:
        """Get transformation strategy description"""
        strategies = {
            'mysql-mongodb': '📊 Step 1: Parse SQL → JSON\n📊 Step 2: Transform JSON → MongoDB Collections',
            'postgresql-json': '📊 Step 1: Parse PostgreSQL → JSON\n📊 Step 2: Output structured JSON',
            'csv-mongodb': '📊 Step 1: Parse CSV → JSON Array\n📊 Step 2: Transform JSON → MongoDB Documents',
            'mysql-json': '📊 Step 1: Parse SQL Tables → JSON\n📊 Step 2: Output normalized JSON structure',
            'json-mysql': '🔄 Step 1: Validate JSON structure\n🔄 Step 2: Generate MySQL schema and data',
            'mongodb-csv': '🔄 Step 1: Parse MongoDB JSON → Normalized JSON\n🔄 Step 2: Flatten JSON → CSV format'
        }

        key = f"{source}-{target}"
        return strategies.get(key,
                              f"🔄 Two-Step Process:\n📊 Step 1: Convert {source.upper()} → JSON\n📊 Step 2: Transform JSON → {target.upper()}")

    def convert_to_json(self, source_format: str, content: Union[str, bytes], filename: str) -> Dict[str, Any]:
        """Convert various formats to JSON (Step 1)"""
        self.log(f"Step 1: Converting {source_format.upper()} to intermediate JSON format...")

        if source_format.lower() == 'csv':
            return self._convert_csv_to_json(content)
        elif source_format.lower() == 'sql':
            return self._convert_sql_to_json(content)
        elif source_format.lower() == 'json':
            return self._validate_and_normalize_json(content)
        elif source_format.lower() == 'xml':
            return self._convert_xml_to_json(content)
        else:
            raise ValueError(f"Unsupported source format: {source_format}")

    def _convert_xml_to_json(self, content: str) -> Dict[str, Any]:
        """Convert XML to JSON"""
        self.log('Parsing XML file...')

        try:
            import xml.etree.ElementTree as ET

            # Parse XML
            root = ET.fromstring(content)

            # Convert XML to dictionary
            xml_dict = self._xml_to_dict(root)

            # Determine structure type
            if isinstance(xml_dict, dict):
                # Check if it looks like a database structure
                if any(isinstance(v, list) for v in xml_dict.values()):
                    # Database-like structure with tables
                    tables = {}
                    for key, value in xml_dict.items():
                        if isinstance(value, list):
                            tables[key] = value
                        elif isinstance(value, dict):
                            tables[key] = [value]

                    return {
                        'type': 'database',
                        'tables': tables,
                        'metadata': {
                            'source': 'xml',
                            'root_element': root.tag,
                            'tableCount': len(tables)
                        }
                    }
                else:
                    # Single document
                    return {
                        'type': 'document',
                        'data': xml_dict,
                        'metadata': {
                            'source': 'xml',
                            'root_element': root.tag
                        }
                    }
            elif isinstance(xml_dict, list):
                # Array of records
                return {
                    'type': 'table',
                    'name': root.tag,
                    'records': xml_dict,
                    'metadata': {
                        'source': 'xml',
                        'root_element': root.tag,
                        'totalRecords': len(xml_dict)
                    }
                }

        except ET.ParseError as e:
            raise ValueError(f"Invalid XML format: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error parsing XML: {str(e)}")

    def _xml_to_dict(self, element) -> Union[Dict[str, Any], List[Any], str]:
        """Recursively convert XML element to dictionary"""
        result = {}

        # Add attributes as keys with @ prefix
        if element.attrib:
            result.update({f"@{k}": v for k, v in element.attrib.items()})

        # Group children by tag name
        children_by_tag = {}
        for child in element:
            tag = child.tag
            if tag not in children_by_tag:
                children_by_tag[tag] = []
            children_by_tag[tag].append(child)

        # Process children
        for tag, children in children_by_tag.items():
            if len(children) == 1:
                # Single child
                child_dict = self._xml_to_dict(children[0])
                if isinstance(child_dict, dict) and not child_dict:
                    # Empty element with text content
                    result[tag] = children[0].text or ""
                else:
                    result[tag] = child_dict
            else:
                # Multiple children with same tag - create array
                result[tag] = [self._xml_to_dict(child) for child in children]

        # If element has text content and no children/attributes
        if element.text and element.text.strip() and not result:
            return element.text.strip()

        # If element has text content and children/attributes
        if element.text and element.text.strip():
            result['#text'] = element.text.strip()

        return result if result else (element.text or "")

    def _convert_csv_to_json(self, content: str) -> Dict[str, Any]:
        """Convert CSV to JSON"""
        self.log('Parsing CSV file...')

        # Use pandas for robust CSV parsing
        try:
            df = pd.read_csv(StringIO(content))
            records = df.to_dict('records')

            # Convert NaN to None
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None

            self.log(f"Converted {len(records)} CSV records to JSON")

            return {
                'type': 'table',
                'name': 'csv_data',
                'records': records,
                'metadata': {
                    'source': 'csv',
                    'totalRecords': len(records),
                    'columns': list(df.columns)
                }
            }
        except Exception as e:
            raise ValueError(f"Failed to parse CSV: {str(e)}")

    def _convert_sql_to_json(self, content: str) -> Dict[str, Any]:
        """Convert SQL to JSON - Enhanced to handle PostgreSQL COPY statements"""
        self.log('Parsing SQL dump file...')

        # Enhanced SQL parsing
        lines = content.strip().split('\n')
        tables = {}
        current_table = None
        current_columns = []

        create_table_count = 0
        insert_statement_count = 0
        copy_statement_count = 0

        i = 0
        while i < len(lines):
            line = lines[i].strip()

            if not line or line.startswith('--') or line.startswith('#'):
                i += 1
                continue

            # Skip SQL directives
            if re.match(r'^(SET|USE|DROP|LOCK|UNLOCK|START|COMMIT|BEGIN)', line, re.IGNORECASE):
                i += 1
                continue

            # Handle PostgreSQL COPY statements
            copy_match = re.match(r'COPY\s+(?:(\w+)\.)?(\w+)\s*\(([^)]+)\)\s*FROM', line, re.IGNORECASE)
            if copy_match:
                schema = copy_match.group(1)  # e.g., 'public'
                table_name = copy_match.group(2)  # e.g., 'netflix_shows'
                columns_str = copy_match.group(3)  # column list

                copy_statement_count += 1
                current_table = table_name

                # Parse column names
                current_columns = [col.strip().replace('"', '').replace('`', '') for col in columns_str.split(',')]
                tables[current_table] = []

                self.log(f"Found COPY statement for table {table_name} with {len(current_columns)} columns")

                # Skip the FROM clause line and any metadata
                i += 1
                while i < len(lines) and not self._is_copy_data_line(lines[i]):
                    i += 1

                # Process data lines until we hit the end marker (\.)
                while i < len(lines):
                    line = lines[i].strip()

                    # End of COPY data
                    if line == '\\.' or line.startswith('\\.'):
                        break

                    # Skip empty lines
                    if not line:
                        i += 1
                        continue

                    # Parse tab-separated values (PostgreSQL COPY format)
                    if line and not line.startswith('--'):
                        record = self._parse_copy_data_line(line, current_columns)
                        if record:
                            tables[current_table].append(record)

                    i += 1

                i += 1
                continue

            # CREATE TABLE detection (existing logic)
            create_match = re.match(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`|")?(\w+)(?:`|")?\s*\(', line,
                                    re.IGNORECASE)

            if create_match:
                current_table = create_match.group(1)
                tables[current_table] = []
                create_table_count += 1

                # Extract column definitions (simplified)
                full_statement = line
                paren_count = line.count('(') - line.count(')')
                j = i + 1
                while j < len(lines) and paren_count > 0:
                    return
                  #  next...



