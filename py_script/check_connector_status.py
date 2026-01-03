#!/usr/bin/env python3
"""
Script untuk cek status CDC connector
"""

import requests
import json

KAFKA_CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "mks-finance-mysql-connector"

def check_connector():
    """Cek status connector"""
    print("=" * 60)
    print("Cek CDC Connector Status")
    print("=" * 60)
    
    try:
        # Cek apakah connector ada
        response = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}")
        if response.status_code == 200:
            print(f"\n✓ Connector {CONNECTOR_NAME} ditemukan")
        else:
            print(f"\n✗ Connector {CONNECTOR_NAME} tidak ditemukan")
            print(f"  Status: {response.status_code}")
            return
        
        # Cek status
        response = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/status")
        if response.status_code == 200:
            status = response.json()
            connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
            print(f"\nStatus Connector: {connector_state}")
            
            if connector_state != 'RUNNING':
                print(f"\n⚠ Connector tidak running!")
                print(f"  Worker ID: {status.get('connector', {}).get('worker_id', 'N/A')}")
                if 'trace' in status.get('connector', {}):
                    print(f"  Error: {status['connector']['trace']}")
            
            # Cek tasks
            tasks = status.get('tasks', [])
            print(f"\nTasks: {len(tasks)}")
            for i, task in enumerate(tasks):
                task_state = task.get('state', 'UNKNOWN')
                task_id = task.get('id', i)
                print(f"  Task {task_id}: {task_state}")
                if task_state != 'RUNNING' and 'trace' in task:
                    print(f"    Error: {task['trace']}")
        else:
            print(f"\n✗ Gagal mendapatkan status: {response.status_code}")
        
        # Cek config
        print("\n" + "=" * 60)
        print("Connector Configuration")
        print("=" * 60)
        response = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/config")
        if response.status_code == 200:
            config = response.json()
            print(f"\nDatabase: {config.get('database.hostname', 'N/A')}:{config.get('database.port', 'N/A')}")
            print(f"Database Name: {config.get('database.include.list', 'N/A')}")
            print(f"Tables: {config.get('table.include.list', 'N/A')}")
        
    except requests.exceptions.ConnectionError:
        print("\n✗ Tidak bisa connect ke Kafka Connect")
        print("  Pastikan Kafka Connect sudah running:")
        print("    docker-compose up -d")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_connector()

