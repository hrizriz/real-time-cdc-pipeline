#!/usr/bin/env python3
"""
Script untuk cek apakah topic Kafka ada dan berapa banyak message di dalamnya
"""

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import sys

import os

KAFKA_BOOTSTRAP_SERVERS = [os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")]
TOPIC_PREFIX = os.environ.get("DEBEZIUM_TOPIC_PREFIX", "mks_finance")
DATABASE_NAME = os.environ.get("DEBEZIUM_DATABASE_NAME", "mks_finance_dw")
TOPICS = [
    f"{TOPIC_PREFIX}.{DATABASE_NAME}.customers",
    f"{TOPIC_PREFIX}.{DATABASE_NAME}.credit_applications",
    f"{TOPIC_PREFIX}.{DATABASE_NAME}.vehicle_ownership"
]

def check_topics():
    """Cek status topic"""
    print("=" * 60)
    print("Cek Kafka Topics")
    print("=" * 60)
    
    try:
        # Buat admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='topic_checker'
        )
        
        # List semua topic
        print("\nMengambil daftar topic...")
        # list_topics() returns a list of topic names
        all_topics = admin_client.list_topics()
        
        print(f"\n✓ Total topic di Kafka: {len(all_topics)}")
        print(f"\nTopic yang dimonitor:")
        for topic in TOPICS:
            if topic in all_topics:
                print(f"  ✓ {topic} - ADA")
            else:
                print(f"  ✗ {topic} - TIDAK ADA")
        
        # Cek jumlah message di setiap topic menggunakan kafka consumer
        print("\n" + "=" * 60)
        print("Cek Jumlah Message di Topic")
        print("=" * 60)
        
        for topic in TOPICS:
            if topic not in all_topics:
                print(f"\n{topic}: Topic tidak ada")
                continue
                
            try:
                # Buat consumer untuk cek offset - langsung assign, jangan subscribe
                from kafka import TopicPartition
                import time
                
                # Get partitions dulu dengan consumer terpisah
                temp_consumer = KafkaConsumer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    consumer_timeout_ms=1000
                )
                partitions = temp_consumer.partitions_for_topic(topic)
                temp_consumer.close()
                
                if not partitions:
                    print(f"\n{topic}: Tidak ada partition")
                    continue
                
                print(f"\n{topic}:")
                total_messages = 0
                
                # Cek setiap partition dengan assign langsung
                for partition in partitions:
                    try:
                        consumer = KafkaConsumer(
                            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                            enable_auto_commit=False,
                            consumer_timeout_ms=2000
                        )
                        
                        tp = TopicPartition(topic, partition)
                        consumer.assign([tp])
                        
                        # Seek to beginning
                        consumer.seek_to_beginning(tp)
                        low_offset = consumer.position(tp)
                        
                        # Seek to end
                        consumer.seek_to_end(tp)
                        high_offset = consumer.position(tp)
                        
                        msg_count = high_offset - low_offset
                        total_messages += msg_count
                        print(f"  Partition {partition}: {msg_count} messages (offset: {low_offset} to {high_offset})")
                        
                        consumer.close()
                    except Exception as e:
                        print(f"  Partition {partition}: Error - {e}")
                
                if total_messages == 0:
                    print(f"  ⚠ Tidak ada message di topic ini")
                else:
                    print(f"  ✓ Total: {total_messages} messages")
                
            except Exception as e:
                print(f"\n{topic}: Error - {e}")
                import traceback
                traceback.print_exc()
        
        admin_client.close()
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    check_topics()

