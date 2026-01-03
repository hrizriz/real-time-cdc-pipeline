#!/usr/bin/env python3
import os
import subprocess

PG_HOST = os.environ.get("ODS_HOST", "localhost")
PG_PORT = int(os.environ.get("ODS_PORT", "5432"))
PG_USER = os.environ.get("ODS_USER", "ods_user")
PG_PASSWORD = os.environ.get("ODS_PASSWORD", "ods_pwd")
PG_DB = os.environ.get("ODS_DB", "ods_db")

def verify_with_psycopg2():
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM customers")
        customer_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM credit_applications")
        app_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM vehicle_ownership")
        vehicle_count = cur.fetchone()[0]
        print("=" * 60)
        print("Verify Data di ODS (PostgreSQL)")
        print("=" * 60)
        print(f"\n✓ Customers: {customer_count} records")
        print(f"✓ Credit Applications: {app_count} records")
        print(f"✓ Vehicle Ownership: {vehicle_count} records")
        if customer_count > 0:
            cur.execute("SELECT customer_id, full_name, last_updated FROM customers ORDER BY last_updated DESC LIMIT 5")
            rows = cur.fetchall()
            print("\nSample Customers (latest 5):")
            for row in rows:
                print(f"  - {row[0]}: {row[1]} (updated: {row[2]})")
        conn.close()
        return True
    except ImportError:
        return False
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False

def run_psql(query):
    cmd = [
        "docker", "exec", "postgres", "sh", "-lc",
        f"PGPASSWORD={PG_PASSWORD} psql -U {PG_USER} -d {PG_DB} -t -A -c \"{query}\""
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()

def verify_with_psql():
    print("=" * 60)
    print("Verify Data di ODS (PostgreSQL via psql in container)")
    print("=" * 60)
    ensure_tables()
    customer_count = int(run_psql("SELECT COUNT(*) FROM customers"))
    app_count = int(run_psql("SELECT COUNT(*) FROM credit_applications"))
    vehicle_count = int(run_psql("SELECT COUNT(*) FROM vehicle_ownership"))
    print(f"\n✓ Customers: {customer_count} records")
    print(f"✓ Credit Applications: {app_count} records")
    print(f"✓ Vehicle Ownership: {vehicle_count} records")
    if customer_count > 0:
        sample = run_psql("SELECT customer_id || '|' || full_name || '|' || COALESCE(last_updated::text,'') FROM customers ORDER BY last_updated DESC LIMIT 5")
        print("\nSample Customers (latest 5):")
        for line in sample.splitlines():
            parts = line.split("|")
            print(f"  - {parts[0]}: {parts[1]} (updated: {parts[2]})")

def table_exists(name: str) -> bool:
    q = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema='public' AND table_name='{name}');"
    out = run_psql(q)
    return out.strip() in ("t", "true", "1")

def ensure_tables():
    if not table_exists("customers"):
        run_psql("""
            CREATE TABLE IF NOT EXISTS customers (
              customer_id TEXT PRIMARY KEY,
              nik TEXT,
              full_name TEXT,
              date_of_birth DATE,
              gender TEXT,
              marital_status TEXT,
              phone_number TEXT,
              email TEXT,
              address TEXT,
              city TEXT,
              province TEXT,
              postal_code TEXT,
              occupation TEXT,
              employer_name TEXT,
              monthly_income NUMERIC(15,2),
              employment_status TEXT,
              years_of_employment INT,
              education_level TEXT,
              emergency_contact_name TEXT,
              emergency_contact_phone TEXT,
              emergency_contact_relation TEXT,
              credit_score INT,
              customer_segment TEXT,
              registration_date TIMESTAMP,
              last_updated TIMESTAMP,
              status TEXT,
              created_by TEXT,
              updated_by TEXT,
              cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              cdc_operation TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_customers_nik ON customers (nik);
            CREATE INDEX IF NOT EXISTS idx_customers_last_updated ON customers (last_updated);
        """)
    if not table_exists("credit_applications"):
        run_psql("""
            CREATE TABLE IF NOT EXISTS credit_applications (
              application_id TEXT PRIMARY KEY,
              customer_id TEXT,
              application_date TIMESTAMP,
              vehicle_type TEXT,
              vehicle_brand TEXT,
              vehicle_model TEXT,
              vehicle_year INT,
              vehicle_price NUMERIC(15,2),
              down_payment NUMERIC(15,2),
              loan_amount NUMERIC(15,2),
              tenor_months INT,
              interest_rate NUMERIC(5,2),
              monthly_installment NUMERIC(15,2),
              application_status TEXT,
              approval_date TIMESTAMP NULL,
              rejection_reason TEXT,
              disbursement_date TIMESTAMP NULL,
              first_installment_date TIMESTAMP NULL,
              last_payment_date TIMESTAMP NULL,
              outstanding_amount NUMERIC(15,2),
              payment_status TEXT,
              collateral_status TEXT,
              notes TEXT,
              processed_by TEXT,
              approved_by TEXT,
              created_date TIMESTAMP,
              cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              cdc_operation TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_credit_applications_customer_id ON credit_applications (customer_id);
            CREATE INDEX IF NOT EXISTS idx_credit_applications_application_date ON credit_applications (application_date);
        """)
    if not table_exists("vehicle_ownership"):
        run_psql("""
            CREATE TABLE IF NOT EXISTS vehicle_ownership (
              ownership_id TEXT PRIMARY KEY,
              customer_id TEXT,
              vehicle_type TEXT,
              brand TEXT,
              model TEXT,
              year INT,
              vehicle_price NUMERIC(15,2),
              purchase_date DATE,
              ownership_status TEXT,
              registration_number TEXT,
              chassis_number TEXT,
              engine_number TEXT,
              created_date TIMESTAMP,
              cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              cdc_operation TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_vehicle_ownership_customer_id ON vehicle_ownership (customer_id);
        """)

def main():
    if not verify_with_psycopg2():
        try:
            verify_with_psql()
        except Exception as e:
            print(f"\n✗ Error: {e}")

if __name__ == "__main__":
    main()

