#!/usr/bin/env python3
"""
Script helper untuk query ODS PostgreSQL dengan mudah
"""

import subprocess
import sys
import os

PG_HOST = os.environ.get("ODS_HOST", "localhost")
PG_PORT = int(os.environ.get("ODS_PORT", "5432"))
PG_USER = os.environ.get("ODS_USER", "ods_user")
PG_PASSWORD = os.environ.get("ODS_PASSWORD", "ods_pwd")
PG_DB = os.environ.get("ODS_DB", "ods_db")

def run_query(query, format_output=True):
    """Run query via docker exec psql"""
    cmd = [
        "docker", "exec", "postgres",
        "psql", "-U", PG_USER, "-d", PG_DB,
        "-c", query
    ]
    
    if format_output:
        cmd.extend(["-x"])  # Expanded display
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        return result.stdout
    else:
        print(f"✗ Error: {result.stderr}")
        return None

def interactive_mode():
    """Interactive psql mode"""
    print("=" * 60)
    print("Interactive ODS Query Mode")
    print("=" * 60)
    print("\nMasuk ke psql interactive mode...")
    print("Gunakan \\q untuk exit\n")
    
    subprocess.run([
        "docker", "exec", "-it", "postgres",
        "psql", "-U", PG_USER, "-d", PG_DB
    ])

def show_tables():
    """Show all tables"""
    print("=" * 60)
    print("ODS Tables")
    print("=" * 60)
    result = run_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
    if result:
        print(result)

def show_counts():
    """Show record counts for all tables"""
    print("=" * 60)
    print("ODS Record Counts")
    print("=" * 60)
    
    queries = [
        ("Customers", "SELECT COUNT(*) as count FROM customers;"),
        ("Credit Applications", "SELECT COUNT(*) as count FROM credit_applications;"),
        ("Vehicle Ownership", "SELECT COUNT(*) as count FROM vehicle_ownership;")
    ]
    
    for name, query in queries:
        result = run_query(query, format_output=False)
        if result:
            # Extract count from output
            lines = result.strip().split('\n')
            for line in lines:
                if line.strip().isdigit():
                    print(f"  {name}: {line.strip()} records")
                    break

def show_sample(table_name, limit=5):
    """Show sample data from table"""
    print("=" * 60)
    print(f"Sample Data: {table_name}")
    print("=" * 60)
    
    query = f"SELECT * FROM {table_name} LIMIT {limit};"
    result = run_query(query)
    if result:
        print(result)

def show_latest_updates(limit=10):
    """Show latest updated records"""
    print("=" * 60)
    print(f"Latest Updates (Top {limit})")
    print("=" * 60)
    
    query = f"""
    SELECT 
        customer_id, 
        full_name, 
        last_updated,
        cdc_operation,
        cdc_timestamp
    FROM customers 
    ORDER BY cdc_timestamp DESC 
    LIMIT {limit};
    """
    
    result = run_query(query)
    if result:
        print(result)

def main():
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "interactive" or command == "i":
            interactive_mode()
        elif command == "tables" or command == "t":
            show_tables()
        elif command == "count" or command == "c":
            show_counts()
        elif command == "sample" or command == "s":
            table = sys.argv[2] if len(sys.argv) > 2 else "customers"
            limit = int(sys.argv[3]) if len(sys.argv) > 3 else 5
            show_sample(table, limit)
        elif command == "latest" or command == "l":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            show_latest_updates(limit)
        elif command == "query" or command == "q":
            if len(sys.argv) > 2:
                query = " ".join(sys.argv[2:])
                result = run_query(query)
                if result:
                    print(result)
            else:
                print("✗ Error: Query required")
                print("Usage: python query_ods.py query \"SELECT * FROM customers LIMIT 5;\"")
        else:
            print_usage()
    else:
        print_usage()

def print_usage():
    print("=" * 60)
    print("ODS Query Helper")
    print("=" * 60)
    print("\nUsage:")
    print("  python query_ods.py [command] [options]")
    print("\nCommands:")
    print("  interactive, i    - Enter interactive psql mode")
    print("  tables, t         - Show all tables")
    print("  count, c          - Show record counts")
    print("  sample, s [table] [limit] - Show sample data (default: customers, limit 5)")
    print("  latest, l [limit] - Show latest updates (default: limit 10)")
    print("  query, q \"SQL\"    - Execute custom SQL query")
    print("\nExamples:")
    print("  python query_ods.py interactive")
    print("  python query_ods.py count")
    print("  python query_ods.py sample customers 10")
    print("  python query_ods.py latest 20")
    print("  python query_ods.py query \"SELECT COUNT(*) FROM customers;\"")
    print("\nAlternative: Access via GUI tools or psql directly")
    print("  docker exec -it postgres psql -U ods_user -d ods_db")

if __name__ == "__main__":
    main()

