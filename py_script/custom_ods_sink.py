#!/usr/bin/env python3
"""
Custom consumer untuk sink data dari Kafka ke ODS PostgreSQL
Mengconvert format Debezium ke format PostgreSQL
"""

import json
import base64
from datetime import datetime, timedelta
import os
import sys

try:
    from kafka import KafkaConsumer
except ImportError:
    print("✗ Error: kafka-python tidak terinstall")
    print("  Install dengan: pip install kafka-python")
    sys.exit(1)

try:
    import psycopg2
except ImportError:
    print("✗ Error: psycopg2 tidak terinstall")
    print("  Install dengan: pip install psycopg2-binary")
    sys.exit(1)

# Kafka config
# Note: Update topic names to match your DEBEZIUM_TOPIC_PREFIX and database name
KAFKA_BOOTSTRAP_SERVERS = [os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")]
TOPIC_PREFIX = os.environ.get("DEBEZIUM_TOPIC_PREFIX", "mks_finance")
DATABASE_NAME = os.environ.get("DEBEZIUM_DATABASE_NAME", "mks_finance_dw")
TOPICS = [
    f"{TOPIC_PREFIX}.{DATABASE_NAME}.customers",
    f"{TOPIC_PREFIX}.{DATABASE_NAME}.credit_applications",
    f"{TOPIC_PREFIX}.{DATABASE_NAME}.vehicle_ownership"
]

# PostgreSQL config
# Note: Update these values in .env file for production
PG_CONFIG = {
    'host': os.environ.get("ODS_HOST", "localhost"),
    'port': int(os.environ.get("ODS_PORT", "5432")),
    'user': os.environ.get("ODS_USER", "ods_user"),
    'password': os.environ.get("ODS_PASSWORD", "ods_pwd"),  # Change in .env file
    'database': os.environ.get("ODS_DB", "ods_db")
}

def convert_debezium_date(epoch_days):
    """Convert Debezium date (epoch days) ke PostgreSQL date"""
    if epoch_days is None:
        return None
    base_date = datetime(1970, 1, 1)
    return (base_date + timedelta(days=epoch_days)).date()

def convert_debezium_timestamp(ts_ms):
    """Convert Debezium timestamp (milliseconds) ke PostgreSQL timestamp"""
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0)

def convert_debezium_decimal(decimal_base64, scale=2):
    """Convert Debezium decimal (base64) ke numeric
    Debezium decimal adalah base64 encoded BigDecimal bytes
    Format: [unscaled_value_bytes] dengan scale
    """
    if decimal_base64 is None:
        return None
    try:
        # Decode base64
        decoded = base64.b64decode(decimal_base64)
        
        # Debezium decimal menggunakan Java BigDecimal serialization
        # Byte pertama adalah scale, sisanya adalah unscaled value (big-endian)
        if len(decoded) < 1:
            return None
        
        # Simplified: coba parse sebagai big-endian integer
        # Note: Ini simplified version, untuk production perlu implementasi lengkap
        if len(decoded) <= 8:
            # Untuk nilai kecil, bisa langsung convert
            value = int.from_bytes(decoded, byteorder='big', signed=True)
            # Apply scale
            return float(value) / (10 ** scale)
        else:
            # Untuk nilai besar, perlu BigInteger handling
            # Untuk sekarang, return None dan log warning
            print(f"⚠ Warning: Decimal value terlalu besar, skip: {decimal_base64[:20]}...")
            return None
    except Exception as e:
        print(f"⚠ Warning: Gagal decode decimal {decimal_base64[:20]}...: {e}")
        return None

def convert_customer_record(payload):
    """Convert Debezium customer record ke format PostgreSQL"""
    record = {}
    
    # Simple fields
    record['customer_id'] = payload.get('customer_id')
    record['nik'] = payload.get('nik')
    record['full_name'] = payload.get('full_name')
    record['gender'] = payload.get('gender')
    record['marital_status'] = payload.get('marital_status')
    record['phone_number'] = payload.get('phone_number')
    record['email'] = payload.get('email')
    record['address'] = payload.get('address')
    record['city'] = payload.get('city')
    record['province'] = payload.get('province')
    record['postal_code'] = payload.get('postal_code')
    record['occupation'] = payload.get('occupation')
    record['employer_name'] = payload.get('employer_name')
    record['employment_status'] = payload.get('employment_status')
    record['years_of_employment'] = payload.get('years_of_employment')
    record['education_level'] = payload.get('education_level')
    record['emergency_contact_name'] = payload.get('emergency_contact_name')
    record['emergency_contact_phone'] = payload.get('emergency_contact_phone')
    record['emergency_contact_relation'] = payload.get('emergency_contact_relation')
    record['credit_score'] = payload.get('credit_score')
    record['customer_segment'] = payload.get('customer_segment')
    record['status'] = payload.get('status', 'Active')
    record['created_by'] = payload.get('created_by', 'SYSTEM')
    record['updated_by'] = payload.get('updated_by', 'SYSTEM')
    
    # Convert special formats
    record['date_of_birth'] = convert_debezium_date(payload.get('date_of_birth'))
    record['registration_date'] = convert_debezium_timestamp(payload.get('registration_date'))
    record['last_updated'] = convert_debezium_timestamp(payload.get('last_updated'))
    
    # Monthly income - decode base64 decimal
    monthly_income = payload.get('monthly_income')
    record['monthly_income'] = convert_debezium_decimal(monthly_income, scale=2)
    
    # CDC metadata
    record['cdc_operation'] = payload.get('__op', 'r')
    record['cdc_timestamp'] = convert_debezium_timestamp(payload.get('__source_ts_ms'))
    
    return record

def insert_customer(conn, record):
    """Insert customer record ke PostgreSQL"""
    cur = conn.cursor()
    
    sql = """
    INSERT INTO customers (
        customer_id, nik, full_name, date_of_birth, gender,
        marital_status, phone_number, email, address, city,
        province, postal_code, occupation, employer_name, monthly_income,
        employment_status, years_of_employment, education_level,
        emergency_contact_name, emergency_contact_phone,
        emergency_contact_relation, credit_score, customer_segment,
        registration_date, last_updated, status, created_by, updated_by,
        cdc_timestamp, cdc_operation
    ) VALUES (
        %(customer_id)s, %(nik)s, %(full_name)s, %(date_of_birth)s, %(gender)s,
        %(marital_status)s, %(phone_number)s, %(email)s, %(address)s, %(city)s,
        %(province)s, %(postal_code)s, %(occupation)s, %(employer_name)s, %(monthly_income)s,
        %(employment_status)s, %(years_of_employment)s, %(education_level)s,
        %(emergency_contact_name)s, %(emergency_contact_phone)s,
        %(emergency_contact_relation)s, %(credit_score)s, %(customer_segment)s,
        %(registration_date)s, %(last_updated)s, %(status)s, %(created_by)s, %(updated_by)s,
        %(cdc_timestamp)s, %(cdc_operation)s
    ) ON CONFLICT (customer_id) DO UPDATE SET
        nik = EXCLUDED.nik,
        full_name = EXCLUDED.full_name,
        date_of_birth = EXCLUDED.date_of_birth,
        gender = EXCLUDED.gender,
        marital_status = EXCLUDED.marital_status,
        phone_number = EXCLUDED.phone_number,
        email = EXCLUDED.email,
        address = EXCLUDED.address,
        city = EXCLUDED.city,
        province = EXCLUDED.province,
        postal_code = EXCLUDED.postal_code,
        occupation = EXCLUDED.occupation,
        employer_name = EXCLUDED.employer_name,
        monthly_income = EXCLUDED.monthly_income,
        employment_status = EXCLUDED.employment_status,
        years_of_employment = EXCLUDED.years_of_employment,
        education_level = EXCLUDED.education_level,
        emergency_contact_name = EXCLUDED.emergency_contact_name,
        emergency_contact_phone = EXCLUDED.emergency_contact_phone,
        emergency_contact_relation = EXCLUDED.emergency_contact_relation,
        credit_score = EXCLUDED.credit_score,
        customer_segment = EXCLUDED.customer_segment,
        registration_date = EXCLUDED.registration_date,
        last_updated = EXCLUDED.last_updated,
        status = EXCLUDED.status,
        updated_by = EXCLUDED.updated_by,
        cdc_timestamp = EXCLUDED.cdc_timestamp,
        cdc_operation = EXCLUDED.cdc_operation;
    """
    
    try:
        cur.execute(sql, record)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"✗ Error inserting customer {record.get('customer_id')}: {e}")
        return False
    finally:
        cur.close()

def convert_credit_application_record(payload):
    """Convert Debezium credit_application record ke format PostgreSQL"""
    record = {}
    
    # Simple fields
    record['application_id'] = payload.get('application_id')
    record['customer_id'] = payload.get('customer_id')
    record['vehicle_type'] = payload.get('vehicle_type')
    record['vehicle_brand'] = payload.get('vehicle_brand')
    record['vehicle_model'] = payload.get('vehicle_model')
    record['vehicle_year'] = payload.get('vehicle_year')
    record['tenor_months'] = payload.get('tenor_months')
    record['application_status'] = payload.get('application_status')
    record['rejection_reason'] = payload.get('rejection_reason')
    record['payment_status'] = payload.get('payment_status', 'Current')
    record['collateral_status'] = payload.get('collateral_status', 'Held')
    record['notes'] = payload.get('notes')
    record['processed_by'] = payload.get('processed_by')
    record['approved_by'] = payload.get('approved_by')
    
    # Convert timestamps
    record['application_date'] = convert_debezium_timestamp(payload.get('application_date'))
    record['approval_date'] = convert_debezium_timestamp(payload.get('approval_date'))
    record['created_date'] = convert_debezium_timestamp(payload.get('created_date'))
    
    # Convert dates (epoch days) - jika None, tetap None
    disbursement_date = payload.get('disbursement_date')
    record['disbursement_date'] = convert_debezium_date(disbursement_date) if disbursement_date else None
    
    first_installment_date = payload.get('first_installment_date')
    record['first_installment_date'] = convert_debezium_date(first_installment_date) if first_installment_date else None
    
    last_payment_date = payload.get('last_payment_date')
    record['last_payment_date'] = convert_debezium_date(last_payment_date) if last_payment_date else None
    
    # Convert decimals
    record['vehicle_price'] = convert_debezium_decimal(payload.get('vehicle_price'), scale=2)
    record['down_payment'] = convert_debezium_decimal(payload.get('down_payment'), scale=2)
    record['loan_amount'] = convert_debezium_decimal(payload.get('loan_amount'), scale=2)
    record['interest_rate'] = convert_debezium_decimal(payload.get('interest_rate'), scale=2)
    record['monthly_installment'] = convert_debezium_decimal(payload.get('monthly_installment'), scale=2)
    record['outstanding_amount'] = convert_debezium_decimal(payload.get('outstanding_amount'), scale=2)
    
    # CDC metadata
    record['cdc_operation'] = payload.get('__op', 'r')
    record['cdc_timestamp'] = convert_debezium_timestamp(payload.get('__source_ts_ms'))
    
    return record

def insert_credit_application(conn, record):
    """Insert credit_application record ke PostgreSQL"""
    cur = conn.cursor()
    
    # Handle None values untuk timestamps
    if record.get('approval_date') is None:
        record['approval_date'] = None
    if record.get('created_date') is None:
        record['created_date'] = None
    
    sql = """
    INSERT INTO credit_applications (
        application_id, customer_id, application_date, vehicle_type, vehicle_brand,
        vehicle_model, vehicle_year, vehicle_price, down_payment, loan_amount,
        tenor_months, interest_rate, monthly_installment, application_status,
        approval_date, rejection_reason, disbursement_date, first_installment_date,
        last_payment_date, outstanding_amount, payment_status, collateral_status,
        notes, processed_by, approved_by, created_date, cdc_timestamp, cdc_operation
    ) VALUES (
        %(application_id)s, %(customer_id)s, %(application_date)s, %(vehicle_type)s, %(vehicle_brand)s,
        %(vehicle_model)s, %(vehicle_year)s, %(vehicle_price)s, %(down_payment)s, %(loan_amount)s,
        %(tenor_months)s, %(interest_rate)s, %(monthly_installment)s, %(application_status)s,
        %(approval_date)s, %(rejection_reason)s, %(disbursement_date)s, %(first_installment_date)s,
        %(last_payment_date)s, %(outstanding_amount)s, %(payment_status)s, %(collateral_status)s,
        %(notes)s, %(processed_by)s, %(approved_by)s, %(created_date)s, %(cdc_timestamp)s, %(cdc_operation)s
    ) ON CONFLICT (application_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        application_date = EXCLUDED.application_date,
        vehicle_type = EXCLUDED.vehicle_type,
        vehicle_brand = EXCLUDED.vehicle_brand,
        vehicle_model = EXCLUDED.vehicle_model,
        vehicle_year = EXCLUDED.vehicle_year,
        vehicle_price = EXCLUDED.vehicle_price,
        down_payment = EXCLUDED.down_payment,
        loan_amount = EXCLUDED.loan_amount,
        tenor_months = EXCLUDED.tenor_months,
        interest_rate = EXCLUDED.interest_rate,
        monthly_installment = EXCLUDED.monthly_installment,
        application_status = EXCLUDED.application_status,
        approval_date = EXCLUDED.approval_date,
        rejection_reason = EXCLUDED.rejection_reason,
        disbursement_date = EXCLUDED.disbursement_date,
        first_installment_date = EXCLUDED.first_installment_date,
        last_payment_date = EXCLUDED.last_payment_date,
        outstanding_amount = EXCLUDED.outstanding_amount,
        payment_status = EXCLUDED.payment_status,
        collateral_status = EXCLUDED.collateral_status,
        notes = EXCLUDED.notes,
        processed_by = EXCLUDED.processed_by,
        approved_by = EXCLUDED.approved_by,
        created_date = EXCLUDED.created_date,
        cdc_timestamp = EXCLUDED.cdc_timestamp,
        cdc_operation = EXCLUDED.cdc_operation;
    """
    
    try:
        cur.execute(sql, record)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        app_id = record.get('application_id', 'UNKNOWN')
        print(f"✗ Error inserting credit_application {app_id}: {e}")
        # Print record untuk debugging (hanya untuk beberapa error pertama)
        if 'application_id' in record and record['application_id']:
            print(f"  Record: application_id={record.get('application_id')}, customer_id={record.get('customer_id')}")
            print(f"  application_date={record.get('application_date')}, approval_date={record.get('approval_date')}")
        return False
    finally:
        cur.close()

def convert_vehicle_ownership_record(payload):
    """Convert Debezium vehicle_ownership record ke format PostgreSQL"""
    record = {}
    
    # Simple fields
    record['ownership_id'] = payload.get('ownership_id')
    record['customer_id'] = payload.get('customer_id')
    record['vehicle_type'] = payload.get('vehicle_type')
    record['brand'] = payload.get('brand')
    record['model'] = payload.get('model')
    record['year'] = payload.get('year')
    record['ownership_status'] = payload.get('ownership_status')
    record['registration_number'] = payload.get('registration_number')
    record['chassis_number'] = payload.get('chassis_number')
    record['engine_number'] = payload.get('engine_number')
    
    # Convert special formats
    record['vehicle_price'] = convert_debezium_decimal(payload.get('vehicle_price'), scale=2)
    record['purchase_date'] = convert_debezium_date(payload.get('purchase_date'))
    record['created_date'] = convert_debezium_timestamp(payload.get('created_date'))
    
    # CDC metadata
    record['cdc_operation'] = payload.get('__op', 'r')
    record['cdc_timestamp'] = convert_debezium_timestamp(payload.get('__source_ts_ms'))
    
    return record

def insert_vehicle_ownership(conn, record):
    """Insert vehicle_ownership record ke PostgreSQL"""
    cur = conn.cursor()
    
    # Handle None values
    if record.get('created_date') is None:
        record['created_date'] = None
    
    sql = """
    INSERT INTO vehicle_ownership (
        ownership_id, customer_id, vehicle_type, brand, model,
        year, vehicle_price, purchase_date, ownership_status,
        registration_number, chassis_number, engine_number,
        created_date, cdc_timestamp, cdc_operation
    ) VALUES (
        %(ownership_id)s, %(customer_id)s, %(vehicle_type)s, %(brand)s, %(model)s,
        %(year)s, %(vehicle_price)s, %(purchase_date)s, %(ownership_status)s,
        %(registration_number)s, %(chassis_number)s, %(engine_number)s,
        %(created_date)s, %(cdc_timestamp)s, %(cdc_operation)s
    ) ON CONFLICT (ownership_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        vehicle_type = EXCLUDED.vehicle_type,
        brand = EXCLUDED.brand,
        model = EXCLUDED.model,
        year = EXCLUDED.year,
        vehicle_price = EXCLUDED.vehicle_price,
        purchase_date = EXCLUDED.purchase_date,
        ownership_status = EXCLUDED.ownership_status,
        registration_number = EXCLUDED.registration_number,
        chassis_number = EXCLUDED.chassis_number,
        engine_number = EXCLUDED.engine_number,
        created_date = EXCLUDED.created_date,
        cdc_timestamp = EXCLUDED.cdc_timestamp,
        cdc_operation = EXCLUDED.cdc_operation;
    """
    
    try:
        cur.execute(sql, record)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        own_id = record.get('ownership_id', 'UNKNOWN')
        print(f"✗ Error inserting vehicle_ownership {own_id}: {e}")
        # Print record untuk debugging (hanya untuk beberapa error pertama)
        if 'ownership_id' in record and record['ownership_id']:
            print(f"  Record: ownership_id={record.get('ownership_id')}, customer_id={record.get('customer_id')}")
            print(f"  purchase_date={record.get('purchase_date')}, created_date={record.get('created_date')}")
        return False
    finally:
        cur.close()

def main():
    print("=" * 60)
    print("Custom ODS Sink Consumer")
    print("=" * 60)
    print("\n⚠ PERINGATAN: Script ini akan consume dari Kafka")
    print("Pastikan sink connectors sudah di-stop untuk menghindari duplicate processing")
    print("\nTekan Ctrl+C untuk stop")
    
    # Connect ke PostgreSQL
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        print("\n✓ Connected ke PostgreSQL")
    except Exception as e:
        print(f"\n✗ Gagal connect ke PostgreSQL: {e}")
        sys.exit(1)
    
    # Create Kafka consumer
    # Gunakan group_id yang berbeda setiap kali untuk force read from beginning
    import time
    group_id = f'custom-ods-sink-{int(time.time())}'
    
    print(f"\n✓ Consumer group: {group_id} (baru setiap run)")
    
    # Buat consumer tanpa subscribe (akan assign manual)
    from kafka import TopicPartition
    
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # Disable auto commit untuk kontrol manual
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        consumer_timeout_ms=30000  # Timeout 30 detik jika tidak ada message
    )
    
    print(f"\n✓ Connected ke Kafka")
    print(f"✓ Listening topics: {', '.join(TOPICS)}")
    
    # Cek apakah ada message di topics dan dapatkan partitions
    print("\nChecking topics for partitions...")
    topic_partitions = []
    
    # Subscribe dulu untuk mendapatkan metadata, lalu unsubscribe
    consumer.subscribe(TOPICS)
    consumer.poll(timeout_ms=2000)  # Trigger metadata fetch
    
    for topic in TOPICS:
        partitions = consumer.partitions_for_topic(topic)
        if partitions:
            for partition in partitions:
                tp = TopicPartition(topic, partition)
                topic_partitions.append(tp)
            print(f"  ✓ Topic {topic}: {len(partitions)} partition(s)")
        else:
            print(f"  ⚠ Topic {topic}: tidak ditemukan")
    
    # Unsubscribe sebelum assign
    consumer.unsubscribe()
    
    if not topic_partitions:
        print("\n⚠ Tidak ada partition ditemukan. Cek apakah topics ada di Kafka.")
        consumer.close()
        conn.close()
        sys.exit(1)
    
    # Assign partitions (manual assignment - tidak bisa combine dengan subscribe)
    consumer.assign(topic_partitions)
    
    # Seek to beginning untuk semua partitions
    print("\nSeeking to beginning of all partitions...")
    consumer.seek_to_beginning()
    
    # Cek current position dan end offset
    print("\nPartition positions:")
    for tp in topic_partitions:
        position = consumer.position(tp)
        # Get end offset untuk cek apakah ada message
        end_offset = consumer.end_offsets([tp])[tp]
        print(f"  {tp.topic}[{tp.partition}]: position = {position}, end_offset = {end_offset}")
        if end_offset == 0:
            print(f"    ⚠ Partition kosong (tidak ada message)")
        elif position >= end_offset:
            print(f"    ⚠ Sudah di akhir (tidak ada message baru)")
    
    print("\nProcessing messages...\n")
    
    customer_count = 0
    app_count = 0
    vehicle_count = 0
    message_count = 0
    empty_poll_count = 0
    max_empty_polls = 10  # Stop setelah 10 kali poll kosong berturut-turut
    
    try:
        while True:
            # Poll messages dengan timeout
            msg_pack = consumer.poll(timeout_ms=1000, max_records=100)
            
            if not msg_pack:
                empty_poll_count += 1
                if empty_poll_count % 5 == 0:
                    print(f"  Waiting for messages... (empty polls: {empty_poll_count})")
                if empty_poll_count >= max_empty_polls:
                    print(f"\n⚠ Tidak ada message baru setelah {max_empty_polls} kali poll")
                    print(f"   Total messages processed: {message_count}")
                    break
                continue
            
            empty_poll_count = 0  # Reset counter jika ada message
            
            # Process semua messages dalam batch
            for topic_partition, messages in msg_pack.items():
                for message in messages:
                    message_count += 1
                    topic = message.topic
                    value = message.value
                    
                    if not value:
                        print(f"⚠ Message {message_count} di {message.topic}: value is None")
                        continue
                    
                    if 'payload' not in value:
                        print(f"⚠ Message {message_count} di {message.topic}: tidak ada payload")
                        print(f"   Keys: {list(value.keys()) if isinstance(value, dict) else type(value)}")
                        continue
                    
                    payload = value['payload']
                    
                    # Process berdasarkan topic
                    if 'customers' in topic:
                        try:
                            record = convert_customer_record(payload)
                            if record and record.get('customer_id'):
                                if insert_customer(conn, record):
                                    customer_count += 1
                                    if (customer_count + app_count + vehicle_count) % 10 == 0:
                                        print(f"✓ Processed: {customer_count} customers, {app_count} applications, {vehicle_count} vehicles...")
                        except Exception as e:
                            print(f"✗ Error processing customer message {message_count}: {e}")
                            if message_count <= 5:
                                import traceback
                                traceback.print_exc()
                    
                    elif 'credit_applications' in topic:
                        try:
                            record = convert_credit_application_record(payload)
                            if record and record.get('application_id'):
                                if insert_credit_application(conn, record):
                                    app_count += 1
                                    if (customer_count + app_count + vehicle_count) % 10 == 0:
                                        print(f"✓ Processed: {customer_count} customers, {app_count} applications, {vehicle_count} vehicles...")
                            else:
                                if message_count % 50 == 0:
                                    print(f"⚠ Credit application record tanpa application_id: {payload.get('application_id', 'N/A')}")
                        except Exception as e:
                            print(f"✗ Error processing credit_application message {message_count}: {e}")
                            if message_count <= 5:  # Print detail untuk 5 error pertama
                                import traceback
                                traceback.print_exc()
                    
                    elif 'vehicle_ownership' in topic:
                        try:
                            record = convert_vehicle_ownership_record(payload)
                            if record and record.get('ownership_id'):
                                if insert_vehicle_ownership(conn, record):
                                    vehicle_count += 1
                                    if (customer_count + app_count + vehicle_count) % 10 == 0:
                                        print(f"✓ Processed: {customer_count} customers, {app_count} applications, {vehicle_count} vehicles...")
                            else:
                                if message_count % 50 == 0:
                                    print(f"⚠ Vehicle ownership record tanpa ownership_id: {payload.get('ownership_id', 'N/A')}")
                        except Exception as e:
                            print(f"✗ Error processing vehicle_ownership message {message_count}: {e}")
                            if message_count <= 5:  # Print detail untuk 5 error pertama
                                import traceback
                                traceback.print_exc()
            
            # Commit offset setelah batch
            consumer.commit()
            
    except KeyboardInterrupt:
        total = customer_count + app_count + vehicle_count
        print(f"\n\n✓ Stopped. Total messages received: {message_count}")
        print(f"  Total processed: {total} records")
        print(f"  - Customers: {customer_count}")
        print(f"  - Credit Applications: {app_count}")
        print(f"  - Vehicle Ownership: {vehicle_count}")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print(f"  Messages received: {message_count}")
        print(f"  Records processed: {customer_count + app_count + vehicle_count}")
        import traceback
        traceback.print_exc()
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()

