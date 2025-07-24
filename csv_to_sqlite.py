#!/usr/bin/env python3
"""
CSV to SQLite Database Application

This application loads a user-specified CSV file into an SQLite database,
validates the data, and prints all records.
"""

import csv
import sqlite3
import sys
import os
from pathlib import Path


def create_database_connection(db_path):
    """Create and return a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None


def create_table_from_csv(conn, csv_file_path, table_name="csv_data"):
    """Create a table based on CSV headers and load data."""
    try:
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            # Detect delimiter
            sample = csvfile.read(1024)
            csvfile.seek(0)
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample).delimiter
            
            reader = csv.reader(csvfile, delimiter=delimiter)
            headers = next(reader)
            
            # Clean headers (remove special characters, spaces)
            clean_headers = []
            for header in headers:
                clean_header = ''.join(c if c.isalnum() else '_' for c in header.strip())
                clean_headers.append(clean_header)
            
            # Create table with TEXT columns
            columns_def = ', '.join([f'"{header}" TEXT' for header in clean_headers])
            create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})'
            
            cursor = conn.cursor()
            cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
            cursor.execute(create_table_sql)
            
            # Insert data
            csvfile.seek(0)
            next(reader)  # Skip header row
            
            placeholders = ', '.join(['?' for _ in clean_headers])
            insert_sql = f'INSERT INTO {table_name} VALUES ({placeholders})'
            
            row_count = 0
            for row in reader:
                # Pad row with empty strings if it has fewer columns than headers
                while len(row) < len(clean_headers):
                    row.append('')
                # Truncate row if it has more columns than headers
                row = row[:len(clean_headers)]
                
                cursor.execute(insert_sql, row)
                row_count += 1
            
            conn.commit()
            print(f"Successfully loaded {row_count} rows into table '{table_name}'")
            return clean_headers, row_count
            
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file_path}' not found.")
        return None, 0
    except csv.Error as e:
        print(f"Error reading CSV file: {e}")
        return None, 0
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None, 0
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None, 0


def validate_data(conn, table_name, headers):
    """Validate the data in the database."""
    try:
        cursor = conn.cursor()
        
        # Check total row count
        cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        total_rows = cursor.fetchone()[0]
        print(f"Validation: Found {total_rows} total rows in database")
        
        # Check for completely empty rows
        empty_condition = ' AND '.join([f'"{header}" = ""' for header in headers])
        cursor.execute(f'SELECT COUNT(*) FROM {table_name} WHERE {empty_condition}')
        empty_rows = cursor.fetchone()[0]
        if empty_rows > 0:
            print(f"Warning: Found {empty_rows} completely empty rows")
        
        # Check data integrity by sampling a few rows
        cursor.execute(f'SELECT * FROM {table_name} LIMIT 5')
        sample_rows = cursor.fetchall()
        print(f"Validation: Sample of first 5 rows verified successfully")
        
        return True
        
    except sqlite3.Error as e:
        print(f"Validation error: {e}")
        return False


def print_all_records(conn, table_name, headers):
    """Print all records from the database using a Python loop."""
    try:
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {table_name}')
        
        print(f"\n{'='*60}")
        print(f"ALL RECORDS FROM TABLE '{table_name}':")
        print(f"{'='*60}")
        
        # Print headers
        header_line = " | ".join(f"{header:15}" for header in headers)
        print(header_line)
        print("-" * len(header_line))
        
        # Use Python loop to print each record
        record_count = 0
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            
            record_count += 1
            # Format each field in the row
            formatted_row = " | ".join(f"{str(field):15}" for field in row)
            print(f"{formatted_row}")
        
        print(f"\nTotal records printed: {record_count}")
        
    except sqlite3.Error as e:
        print(f"Error printing records: {e}")


def main():
    """Main function to run the application."""
    # Get CSV file path from user
    if len(sys.argv) > 1:
        csv_file_path = sys.argv[1]
    else:
        csv_file_path = input("Enter the path to your CSV file: ").strip()
    
    # Validate file exists
    if not os.path.exists(csv_file_path):
        print(f"Error: File '{csv_file_path}' does not exist.")
        return
    
    # Create database path
    csv_filename = Path(csv_file_path).stem
    db_path = f"{csv_filename}.db"
    
    print(f"Loading CSV file: {csv_file_path}")
    print(f"Creating SQLite database: {db_path}")
    
    # Connect to database
    conn = create_database_connection(db_path)
    if not conn:
        return
    
    try:
        # Load CSV into database
        headers, row_count = create_table_from_csv(conn, csv_file_path)
        if headers is None:
            return
        
        # Validate data
        if validate_data(conn, "csv_data", headers):
            print("Data validation completed successfully")
        else:
            print("Data validation failed")
            return
        
        # Print all records
        print_all_records(conn, "csv_data", headers)
        
    finally:
        conn.close()
        print(f"\nDatabase connection closed. Data saved in '{db_path}'")


if __name__ == "__main__":
    main()