#!/usr/bin/env python3
"""
Script untuk consume CDC events dari Kafka
Menampilkan perubahan data dari tabel customers, credit_applications, dan vehicle_ownership
"""

from kafka import KafkaConsumer
import json
import sys
from datetime import datetime

# Konfigurasi Kafka
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_PREFIX = "mks_finance.mks_finance_dw"

# Tabel yang akan di-monitor
TABLES = {
    "customers": "mks_finance.mks_finance_dw.customers",
    "credit_applications": "mks_finance.mks_finance_dw.credit_applications",
    "vehicle_ownership": "mks_finance.mks_finance_dw.vehicle_ownership"
}

def format_timestamp(ts_ms):
    """Format timestamp dari milliseconds ke datetime"""
    if ts_ms:
        return datetime.fromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
    return "N/A"

def print_event(event_data, operation, topic_name=None):
    """Print event dengan format yang rapi"""
    print("\n" + "=" * 80)
    print(f"OPERATION: {operation}")
    
    # Setelah unwrap dengan add.fields="op,source.ts_ms", field ts_ms ada di root sebagai "source.ts_ms"
    # Tapi karena ada dot, mungkin jadi nested atau flat
    ts_ms = None
    table = 'N/A'
    
    # Cek berbagai kemungkinan lokasi timestamp
    if 'source.ts_ms' in event_data:
        ts_ms = event_data.get('source.ts_ms')
    elif '__source_ts_ms' in event_data:
        ts_ms = event_data.get('__source_ts_ms')
    else:
        # Cek di source object
        source = event_data.get('__source') or event_data.get('source', {})
        if isinstance(source, dict):
            ts_ms = source.get('ts_ms') or source.get('__source_ts_ms')
            table = source.get('table', 'N/A')
    
    # Jika table masih N/A, coba ambil dari topic name
    if table == 'N/A' and topic_name:
        # Format topic: mks_finance.mks_finance_dw.customers
        parts = topic_name.split('.')
        if len(parts) >= 3:
            table = parts[-1]  # Ambil bagian terakhir (customers)
    
    print(f"Timestamp: {format_timestamp(ts_ms)}")
    print(f"Table: {table}")
    print("-" * 80)
    
    # Setelah unwrap, data langsung di root event_data, bukan di after/before
    # Tapi kita perlu filter metadata fields
    metadata_fields = {'__op', '__source', '__deleted', '__source_ts_ms', 'op', 'source', 'before', 'after'}
    
    if operation == "CREATE" or operation == "UPDATE" or operation == "READ":
        print("Data:")
        # Cek apakah ada 'after' (format asli) atau data langsung di root (setelah unwrap)
        data = event_data.get('after', {})
        if not data:
            # Data langsung di root setelah unwrap
            data = {k: v for k, v in event_data.items() if k not in metadata_fields}
        
        for key, value in data.items():
            print(f"  {key}: {value}")
    elif operation == "DELETE":
        print("Data yang dihapus:")
        data = event_data.get('before', {})
        if not data:
            # Data langsung di root setelah unwrap
            data = {k: v for k, v in event_data.items() if k not in metadata_fields}
        
        for key, value in data.items():
            print(f"  {key}: {value}")
    
    print("=" * 80)

def consume_events():
    print("Menghubungkan ke Kafka...")
    
    consumer = None
    try:
        # Buat consumer untuk semua topic
        topics = list(TABLES.values())
        import uuid
        consumer_group = f"cdc-consumer-{uuid.uuid4().hex[:8]}"  # Unique group ID setiap kali
        
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=consumer_group,  # Unique group untuk setiap session
            auto_offset_reset='latest',  # Mulai dari event terbaru setelah consumer start
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
            consumer_timeout_ms=1000  # Timeout 1 detik per poll
        )
        
        print(f"✓ Terhubung ke Kafka")
        print(f"✓ Consumer Group: {consumer_group}")
        print(f"✓ Monitoring topics: {', '.join(topics)}")
        print("\nMenunggu perubahan data... (Ctrl+C untuk stop)")
        print("Note: Hanya akan menerima event BARU setelah consumer ini dijalankan\n")
        
        event_count = 0
        poll_count = 0
        
        # Poll untuk event baru
        while True:
            try:
                message_pack = consumer.poll(timeout_ms=1000)
                poll_count += 1
                
                # Print status setiap 10 poll (sekitar 10 detik)
                if poll_count % 10 == 0:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Masih menunggu... (poll #{poll_count})")
                
                if not message_pack:
                    continue
                
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        try:
                            event_data = message.value
                            
                            # Skip jika value None (tombstone message)
                            if event_data is None:
                                continue
                            
                            topic = message.topic
                            
                            # Skip schema change events
                            if 'schema' in topic or 'history' in topic:
                                continue
                            
                            # Validasi event_data adalah dict
                            if not isinstance(event_data, dict):
                                print(f"⚠ Skipping non-dict message: {type(event_data)}")
                                continue
                            
                            # Debezium message format: {schema: {...}, payload: {...}}
                            # Jika ada 'payload', gunakan payload sebagai event_data
                            if 'payload' in event_data:
                                event_data = event_data['payload']
                            
                            # Skip jika payload None (tombstone setelah unwrap)
                            if event_data is None:
                                continue
                            
                            # Tentukan operation type
                            # Transform unwrap menggunakan __op, bukan op
                            operation = event_data.get('__op') or event_data.get('op', 'UNKNOWN')
                            op_map = {
                                'c': 'CREATE',
                                'u': 'UPDATE',
                                'd': 'DELETE',
                                'r': 'READ'  # Snapshot read
                            }
                            operation = op_map.get(operation, operation)
                            
                            # Skip READ operations (snapshot) jika tidak ingin melihat initial data
                            # Uncomment baris berikut jika ingin skip snapshot reads:
                            # if operation == 'READ':
                            #     continue
                            
                            # Skip jika operation masih UNKNOWN dan tidak ada data
                            # Setelah unwrap, data langsung di root, bukan di after/before
                            has_data = any(key not in ['__op', '__source', '__deleted', '__source_ts_ms'] 
                                         for key in event_data.keys()) if isinstance(event_data, dict) else False
                            if operation == 'UNKNOWN' and not has_data:
                                continue
                            
                            # Print event
                            print_event(event_data, operation, topic)
                            event_count += 1
                            
                        except json.JSONDecodeError as e:
                            print(f"Error parsing JSON: {e}")
                        except Exception as e:
                            print(f"Error processing message: {e}")
                            import traceback
                            traceback.print_exc()
            except Exception as e:
                print(f"Error dalam polling: {e}")
                import traceback
                traceback.print_exc()
                break
        
    except KeyboardInterrupt:
        print("\n\nStopped by user")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("\nPastikan:")
        print("  1. Kafka sudah running (docker-compose up)")
        print("  2. CDC connector sudah di-setup (python setup_cdc.py)")
        sys.exit(1)
    finally:
        try:
            if consumer is not None:
                consumer.close()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        consume_events()
    except KeyboardInterrupt:
        print("\n\nStopped by user")
        sys.exit(0)


