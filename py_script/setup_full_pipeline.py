#!/usr/bin/env python3
"""
Script lengkap untuk setup pipeline dari awal:
1. Docker Compose up
2. Setup CDC (Debezium MySQL Connector)
3. Setup ODS Sink (Custom Consumer)
"""

import subprocess
import time
import sys
import os

def run_command(cmd, description, check=True):
    """Run command dan tampilkan output"""
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    
    result = subprocess.run(
        cmd,
        shell=isinstance(cmd, str),
        capture_output=False,
        text=True
    )
    
    if check and result.returncode != 0:
        print(f"\n✗ Error: Command failed with exit code {result.returncode}")
        return False
    
    return True

def wait_for_service(service_name, check_cmd, max_wait=120):
    """Wait for service to be ready"""
    print(f"\nWaiting for {service_name} to be ready...")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        result = subprocess.run(
            check_cmd,
            shell=True,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(f"✓ {service_name} is ready!")
            return True
        
        time.sleep(2)
        print(".", end="", flush=True)
    
    print(f"\n✗ {service_name} tidak ready setelah {max_wait} detik")
    return False

def main():
    print("="*60)
    print("Setup Full Pipeline - CDC to ODS")
    print("="*60)
    print("\nScript ini akan:")
    print("1. Start Docker Compose services")
    print("2. Wait for services to be ready")
    print("3. Setup Debezium MySQL CDC Connector")
    print("4. Verify CDC is working")
    print("5. Run Custom ODS Sink Consumer")
    print("\n⚠ PERINGATAN: Pastikan tidak ada sink connectors yang running")
    print("Tekan Ctrl+C untuk cancel, atau Enter untuk lanjut...")
    
    try:
        input()
    except KeyboardInterrupt:
        print("\n\nCancelled.")
        sys.exit(0)
    
    # Step 1: Docker Compose Up
    print("\n" + "="*60)
    print("STEP 1: Starting Docker Compose Services")
    print("="*60)
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, ".."))
    docker_compose_file = os.path.join(project_root, "docker-compose.yml")
    
    if not os.path.exists(docker_compose_file):
        print(f"✗ docker-compose.yml tidak ditemukan di {docker_compose_file}")
        sys.exit(1)
    
    if not run_command(
        f"cd {project_root} && docker-compose up -d",
        "Starting Docker Compose services"
    ):
        sys.exit(1)
    
    # Step 2: Wait for services
    print("\n" + "="*60)
    print("STEP 2: Waiting for Services to be Ready")
    print("="*60)
    
    services = [
        ("MySQL", "docker exec mysql mysqladmin ping -h localhost --silent"),
        ("Kafka", "docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092"),
        ("Kafka Connect", "curl -f http://localhost:8083/connectors"),
        ("PostgreSQL", "docker exec postgres pg_isready -U ods_user")
    ]
    
    for service_name, check_cmd in services:
        if not wait_for_service(service_name, check_cmd):
            print(f"\n✗ Service {service_name} tidak ready. Cek logs:")
            print(f"  docker logs {service_name.lower().replace(' ', '-')}")
            sys.exit(1)
    
    print("\n✓ Semua services ready!")
    
    # Step 3: Setup CDC Connector
    print("\n" + "="*60)
    print("STEP 3: Setup Debezium MySQL CDC Connector")
    print("="*60)
    
    if not run_command(
        f"cd {project_root} && python py_script/setup_cdc.py",
        "Setting up CDC connector"
    ):
        print("\n⚠ CDC connector mungkin sudah ada atau ada error")
        print("  Cek dengan: python py_script/check_connector_status.py")
    
    # Wait a bit for initial snapshot
    print("\nWaiting 10 seconds for initial snapshot...")
    time.sleep(10)
    
    # Step 4: Verify CDC
    print("\n" + "="*60)
    print("STEP 4: Verify CDC is Working")
    print("="*60)
    
    if not run_command(
        f"cd {project_root} && python py_script/check_connector_status.py",
        "Checking CDC connector status"
    ):
        print("\n⚠ Ada masalah dengan CDC connector")
    
    if not run_command(
        f"cd {project_root} && python py_script/check_kafka_topics.py",
        "Checking Kafka topics"
    ):
        print("\n⚠ Ada masalah dengan Kafka topics")
    
    # Step 5: Setup ODS Sink
    print("\n" + "="*60)
    print("STEP 5: Setup ODS Sink")
    print("="*60)
    
    print("\n⚠ PERINGATAN: Pastikan sink connectors sudah di-delete/pause")
    print("  Jika belum, jalankan:")
    print("    curl -X DELETE http://localhost:8083/connectors/jdbc-sink-customers")
    print("    curl -X DELETE http://localhost:8083/connectors/jdbc-sink-credit-applications")
    print("    curl -X DELETE http://localhost:8083/connectors/jdbc-sink-vehicle-ownership")
    
    print("\nSekarang akan menjalankan Custom ODS Sink Consumer...")
    print("Tekan Ctrl+C untuk stop consumer")
    print("\nStarting consumer in 3 seconds...")
    time.sleep(3)
    
    if not run_command(
        f"cd {project_root} && python py_script/custom_ods_sink.py",
        "Running Custom ODS Sink Consumer",
        check=False  # Don't fail if user stops with Ctrl+C
    ):
        print("\n⚠ Consumer stopped")
    
    # Step 6: Verify ODS
    print("\n" + "="*60)
    print("STEP 6: Verify ODS Data")
    print("="*60)
    
    run_command(
        f"cd {project_root} && python py_script/verify_ods.py",
        "Verifying ODS data"
    )
    
    print("\n" + "="*60)
    print("✓ Setup Complete!")
    print("="*60)
    print("\nUntuk verifikasi manual:")
    print("  python py_script/verify_ods.py")
    print("  python py_script/check_kafka_topics.py")
    print("  python py_script/check_connector_status.py")

if __name__ == "__main__":
    main()

