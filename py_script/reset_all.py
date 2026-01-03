#!/usr/bin/env python3
"""
Script untuk reset semua: stop services, cleanup data, delete connectors
"""

import subprocess
import sys
import os
import requests

KAFKA_CONNECT_URL = "http://localhost:8083"

def delete_connector(connector_name):
    """Delete connector"""
    try:
        response = requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}")
        if response.status_code in (200, 204, 404):
            print(f"  ✓ {connector_name} deleted")
            return True
        else:
            print(f"  ⚠ {connector_name}: {response.status_code}")
            return False
    except Exception as e:
        print(f"  ✗ {connector_name}: {e}")
        return False

def main():
    print("="*60)
    print("Reset All - Cleanup Pipeline")
    print("="*60)
    print("\n⚠ PERINGATAN: Script ini akan:")
    print("1. Stop semua Kafka Connect connectors")
    print("2. Stop Docker Compose services")
    print("3. (Optional) Remove volumes (data akan hilang)")
    print("\nTekan Ctrl+C untuk cancel, atau Enter untuk lanjut...")
    
    try:
        input()
    except KeyboardInterrupt:
        print("\n\nCancelled.")
        sys.exit(0)
    
    # Step 1: Delete connectors
    print("\n" + "="*60)
    print("STEP 1: Delete Kafka Connect Connectors")
    print("="*60)
    
    connectors = [
        "jdbc-sink-customers",
        "jdbc-sink-credit-applications",
        "jdbc-sink-vehicle-ownership",
        "mks-finance-mysql-connector"
    ]
    
    for connector in connectors:
        delete_connector(connector)
    
    # Step 2: Stop Docker Compose
    print("\n" + "="*60)
    print("STEP 2: Stop Docker Compose Services")
    print("="*60)
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, ".."))
    docker_compose_file = os.path.join(project_root, "docker-compose.yml")
    
    if os.path.exists(docker_compose_file):
        result = subprocess.run(
            f"cd {project_root} && docker-compose down",
            shell=True,
            capture_output=False,
            text=True
        )
        if result.returncode == 0:
            print("✓ Docker Compose services stopped")
        else:
            print("⚠ Error stopping services")
    else:
        print("⚠ docker-compose.yml tidak ditemukan")
    
    # Step 3: Optional - Remove volumes
    print("\n" + "="*60)
    print("STEP 3: Remove Volumes (Optional)")
    print("="*60)
    print("\nHapus volumes? Ini akan menghapus semua data (MySQL, PostgreSQL, Kafka)")
    print("Tekan 'y' untuk hapus, atau Enter untuk skip...")
    
    try:
        choice = input().strip().lower()
        if choice == 'y':
            result = subprocess.run(
                f"cd {project_root} && docker-compose down -v",
                shell=True,
                capture_output=False,
                text=True
            )
            if result.returncode == 0:
                print("✓ Volumes removed")
            else:
                print("⚠ Error removing volumes")
        else:
            print("✓ Skipped (volumes tetap ada)")
    except KeyboardInterrupt:
        print("\n✓ Skipped")
    
    print("\n" + "="*60)
    print("✓ Reset Complete!")
    print("="*60)
    print("\nUntuk start ulang, jalankan:")
    print("  python py_script/setup_full_pipeline.py")
    print("\natau manual:")
    print("  docker-compose up -d")
    print("  python py_script/setup_cdc.py")
    print("  python py_script/custom_ods_sink.py")

if __name__ == "__main__":
    main()

