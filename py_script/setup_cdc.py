#!/usr/bin/env python3
"""
Script untuk setup CDC connector Debezium untuk MKS Finance (lokasi: py_script/)
"""
import requests
import json
import time
import sys
import os
import argparse
from typing import Optional
import subprocess

KAFKA_CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "mks-finance-mysql-connector"
MYSQL_ROOT_PASSWORD = os.environ.get("MYSQL_ROOT_PASSWORD", "root_password123")  # Change in .env file
MYSQL_DB = os.environ.get("MYSQL_DATABASE", "mks_finance_dw")

def resolve_config_path(cli_path: Optional[str] = None) -> str:
    if cli_path:
        return os.path.abspath(cli_path)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, ".."))
    return os.path.join(project_root, "debezium-connector-config", "mysql-connector.json")
CONNECTOR_CONFIG_FILE = resolve_config_path()

def wait_for_kafka_connect():
    print("Menunggu Kafka Connect siap...")
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get(f"{KAFKA_CONNECT_URL}/connectors")
            if response.status_code == 200:
                print("✓ Kafka Connect siap!")
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(2)
        print(f"  Mencoba lagi... ({i+1}/{max_retries})")
    return False

def check_connector_exists():
    try:
        response = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}")
        return response.status_code == 200
    except:
        return False

def delete_connector():
    try:
        response = requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}")
        if response.status_code == 200 or response.status_code == 204:
            print(f"✓ Connector {CONNECTOR_NAME} dihapus")
            time.sleep(2)
            return True
    except Exception as e:
        print(f"Error saat menghapus connector: {e}")
    return False

def create_connector():
    with open(CONNECTOR_CONFIG_FILE, 'r') as f:
        config = json.load(f)
    # Override name jika berbeda
    if config.get("name") != CONNECTOR_NAME:
        config["name"] = CONNECTOR_NAME
    url = f"{KAFKA_CONNECT_URL}/connectors"
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json=config, headers=headers)
        if response.status_code == 201:
            print(f"✓ Connector {CONNECTOR_NAME} berhasil dibuat!")
            return True
        else:
            print(f"✗ Gagal membuat connector: {response.status_code}")
            print(f"  Response: {response.text}")
            return False
    except Exception as e:
        print(f"✗ Error saat membuat connector: {e}")
        return False

def get_connector_status():
    try:
        response = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/status")
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

def put_connector_config(config_only: dict) -> bool:
    url = f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/config"
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.put(url, json=config_only, headers=headers)
        if response.status_code in (200, 202):
            return True
        print(f"✗ Gagal PUT config: {response.status_code}")
        print(response.text)
        return False
    except Exception as e:
        print(f"✗ Error PUT config: {e}")
        return False

def run_mysql(query: str) -> subprocess.CompletedProcess:
    cmd = [
        "docker", "exec", "mysql", "sh", "-lc",
        f"mysql -uroot -p{MYSQL_ROOT_PASSWORD} -D {MYSQL_DB} -e \"{query}\""
    ]
    return subprocess.run(cmd, capture_output=True, text=True)

def ensure_signal_table():
    q = "CREATE TABLE IF NOT EXISTS debezium_signal (id VARCHAR(64) PRIMARY KEY, type VARCHAR(64) NOT NULL, data TEXT)"
    res = run_mysql(q)
    if res.returncode != 0:
        print(f"✗ Gagal membuat signal table: {res.stderr.strip()}")
        return False
    return True

from typing import List

def trigger_snapshot(collections: List[str]):
    if not ensure_signal_table():
        return False
    full = []
    for c in collections:
        if "." in c:
            full.append(c)
        else:
            full.append(f"{MYSQL_DB}.{c}")
    data_json = json.dumps({"data-collections": full})
    q = f"INSERT INTO debezium_signal (id, type, data) VALUES ('snap-{int(time.time())}','execute-snapshot','{data_json}')"
    res = run_mysql(q)
    if res.returncode != 0:
        print(f"✗ Gagal trigger snapshot: {res.stderr.strip()}")
        return False
    print("✓ Snapshot trigger dikirim")
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", help="Path ke file konfigurasi Debezium MySQL connector (JSON)")
    parser.add_argument("--connect-url", help="URL Kafka Connect, contoh: http://IP_VM:8083")
    parser.add_argument("--recreate", action="store_true", help="Hapus connector jika sudah ada lalu buat ulang (non-interaktif)")
    parser.add_argument("--connector-name", help="Nama connector baru untuk start dari awal (default: mks-finance-mysql-connector)")
    parser.add_argument("--put-enable-incremental", action="store_true", help="Aktifkan incremental snapshot via PUT /config")
    parser.add_argument("--snapshot", help="Comma-separated daftar tabel untuk di-snapshot, contoh: customers,credit_applications")
    args = parser.parse_args()
    global CONNECTOR_CONFIG_FILE
    global KAFKA_CONNECT_URL
    global CONNECTOR_NAME
    CONNECTOR_CONFIG_FILE = resolve_config_path(args.config_file)
    if args.connect_url:
        KAFKA_CONNECT_URL = args.connect_url
    if args.connector_name:
        CONNECTOR_NAME = args.connector_name
    print("=" * 60)
    print("Setup CDC Connector untuk MKS Finance (py_script)")
    print("=" * 60)
    print(f"Config file: {CONNECTOR_CONFIG_FILE}")
    print(f"Kafka Connect: {KAFKA_CONNECT_URL}")
    if not wait_for_kafka_connect():
        print("✗ Kafka Connect tidak tersedia. Pastikan container sudah running.")
        sys.exit(1)
    if check_connector_exists():
        print(f"\nConnector {CONNECTOR_NAME} sudah ada.")
        if args.recreate:
            print("Menghapus dan membuat ulang connector (non-interaktif)...")
            delete_connector()
        else:
            user_input = input("Hapus dan buat ulang? (y/n): ")
            if user_input.lower() == 'y':
                delete_connector()
            else:
                print("Menggunakan connector yang sudah ada.")
                status = get_connector_status()
                if status:
                    print(f"\nStatus: {status.get('connector', {}).get('state', 'UNKNOWN')}")
                sys.exit(0)
    print(f"\nMembuat connector {CONNECTOR_NAME}...")
    if create_connector():
        time.sleep(3)
        status = get_connector_status()
        if status:
            connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
            print(f"\n✓ Status connector: {connector_state}")
            if connector_state == 'RUNNING':
                print("\n✓ CDC Connector berhasil dijalankan!")
                print("\nUntuk melihat perubahan data, jalankan:")
                print("  python consume_cdc_events.py")
                if args.put_enable_incremental:
                    try:
                        with open(CONNECTOR_CONFIG_FILE, "r") as f:
                            raw = json.load(f)
                        config_only = raw.get("config", {})
                        ok = put_connector_config(config_only)
                        if ok:
                            print("✓ PUT config berhasil")
                    except Exception as e:
                        print(f"✗ Gagal PUT config: {e}")
                if args.snapshot:
                    cols = [x.strip() for x in args.snapshot.split(",") if x.strip()]
                    if cols:
                        trigger_snapshot(cols)
            else:
                print(f"\n⚠ Connector dalam state: {connector_state}")
                print("Cek log untuk detail lebih lanjut.")
        else:
            print("\n⚠ Tidak bisa mendapatkan status connector")
    else:
        print("\n✗ Gagal setup connector")
        sys.exit(1)

if __name__ == "__main__":
    main()

