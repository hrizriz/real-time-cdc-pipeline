# Arsitektur Pipeline - CDC to ODS

## Overview

Pipeline ini mengimplementasikan **Change Data Capture (CDC)** dari MySQL ke **Operational Data Store (ODS)** PostgreSQL menggunakan Apache Kafka sebagai message broker.

## Arsitektur Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         SOURCE LAYER                            │
│                                                                 │
│  ┌──────────────┐                                               │
│  │   MySQL      │  mks_finance_dw database                      │
│  │   (Source)  │  - customers                                   │
│  │              │  - credit_applications                         │
│  │  Port: 3306 │  - vehicle_ownership                          │
│  └──────┬───────┘                                               │
│         │                                                        │
│         │ Binary Log (binlog)                                   │
│         │                                                        │
└─────────┼────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CDC LAYER (Debezium)                      │
│                                                                 │
│  ┌──────────────────────────────────────┐                      │
│  │  Debezium MySQL Connector            │                      │
│  │  (Kafka Connect Source Connector)   │                      │
│  │                                      │                      │
│  │  - Monitors MySQL binlog             │                      │
│  │  - Captures INSERT/UPDATE/DELETE     │                      │
│  │  - Converts to Kafka messages        │                      │
│  └──────────────┬───────────────────────┘                      │
│                 │                                               │
│                 │ Kafka Connect API                             │
│                 │ Port: 8083                                    │
└─────────────────┼───────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MESSAGE BROKER LAYER                         │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │
│  │  Zookeeper   │    │    Kafka     │    │  Kafka UI    │     │
│  │              │    │              │    │              │     │
│  │  Port: 2181  │    │  Port: 9092  │    │  Port: 8080  │     │
│  └──────┬───────┘    └──────┬───────┘    └──────────────┘     │
│         │                   │                                   │
│         └───────────────────┘                                  │
│                                                                 │
│  Topics:                                                        │
│  - mks_finance.mks_finance_dw.customers                        │
│  - mks_finance.mks_finance_dw.credit_applications              │
│  - mks_finance.mks_finance_dw.vehicle_ownership                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Kafka Messages (Debezium Format)
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SINK LAYER                                 │
│                                                                 │
│  ┌──────────────────────────────────────┐                      │
│  │  Custom ODS Sink Consumer           │                      │
│  │  (Python Kafka Consumer)            │                      │
│  │                                      │                      │
│  │  - Consumes from Kafka topics        │                      │
│  │  - Converts Debezium format         │                      │
│  │  - Inserts to PostgreSQL ODS        │                      │
│  └──────────────┬───────────────────────┘                      │
│                 │                                               │
│                 │ JDBC Connection                               │
└─────────────────┼───────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                      TARGET LAYER                                │
│                                                                 │
│  ┌──────────────┐                                               │
│  │  PostgreSQL  │  ods_db database                              │
│  │    (ODS)     │  - customers                                   │
│  │              │  - credit_applications                         │
│  │  Port: 5432  │  - vehicle_ownership                          │
│  └──────────────┘                                               │
│                                                                 │
│  Features:                                                      │
│  - Real-time data replication                                  │
│  - CDC metadata (cdc_operation, cdc_timestamp)                 │
│  - Upsert support (ON CONFLICT)                                │
└─────────────────────────────────────────────────────────────────┘
```

## Komponen Arsitektur

### 1. Source Layer - MySQL

**Tujuan:** Database sumber yang berisi data transaksional

**Komponen:**
- **MySQL 8.0** - Database engine
- **Database:** `mks_finance_dw`
- **Tables:**
  - `customers` - Data pelanggan
  - `credit_applications` - Data aplikasi kredit
  - `vehicle_ownership` - Data kepemilikan kendaraan

**Konfigurasi:**
- Binary logging enabled (untuk CDC)
- Row-based replication format
- User: `app_user` / `app_pwd`

**Lokasi:** `docker-compose.yml` - service `mysql`

---

### 2. CDC Layer - Debezium

**Tujuan:** Capture perubahan data dari MySQL dan publish ke Kafka

**Komponen:**
- **Debezium MySQL Connector** - Kafka Connect Source Connector
- **Kafka Connect** - Framework untuk connectors
- **Image:** `debezium/connect:2.4`

**Fungsi:**
1. Monitor MySQL binary log (binlog)
2. Capture INSERT, UPDATE, DELETE operations
3. Convert ke format Debezium (JSON dengan schema)
4. Publish ke Kafka topics

**Format Message:**
```json
{
  "schema": { ... },
  "payload": {
    "field1": "value1",
    "field2": 123,
    "__op": "c",  // c=create, u=update, d=delete
    "__source_ts_ms": 1234567890,
    "__source_table": "customers"
  }
}
```

**Lokasi:**
- Config: `debezium-connector-config/mysql-connector.json`
- Setup: `py_script/setup_cdc.py`

---

### 3. Message Broker Layer - Apache Kafka

**Tujuan:** Message broker untuk streaming CDC events

**Komponen:**
- **Zookeeper** - Coordination service
- **Kafka** - Distributed streaming platform
- **Kafka UI** - Web UI untuk monitoring

**Topics:**
- `mks_finance.mks_finance_dw.customers`
- `mks_finance.mks_finance_dw.credit_applications`
- `mks_finance.mks_finance_dw.vehicle_ownership`

**Karakteristik:**
- Partition: 1 per topic (untuk development)
- Replication: 1 (untuk development)
- Retention: Default (7 days)
- Format: Debezium JSON with schema

**Lokasi:** `docker-compose.yml` - services `zookeeper`, `kafka`, `kafka-ui`

---

### 4. Sink Layer - Custom Consumer

**Tujuan:** Consume messages dari Kafka dan insert ke PostgreSQL ODS

**Komponen:**
- **Custom Python Consumer** - Kafka consumer dengan format conversion
- **Library:** `kafka-python`, `psycopg2`

**Fungsi:**
1. Consume messages dari Kafka topics
2. Convert format Debezium ke PostgreSQL:
   - Date (epoch days) → PostgreSQL DATE
   - Timestamp (milliseconds) → PostgreSQL TIMESTAMP
   - Decimal (base64) → PostgreSQL NUMERIC
3. Insert/Upsert ke PostgreSQL dengan ON CONFLICT
4. Handle CDC metadata (cdc_operation, cdc_timestamp)

**Consumer Group:**
- Dynamic group ID dengan timestamp: `custom-ods-sink-{timestamp}`
- Auto offset reset: `earliest`
- Manual offset management

**Lokasi:** `py_script/custom_ods_sink.py`

**Mengapa Custom Consumer?**
- Debezium JDBC Sink Connector tidak bisa handle format Debezium dengan benar
- Perlu custom conversion untuk date, timestamp, decimal
- Lebih fleksibel untuk business logic

---

### 5. Target Layer - PostgreSQL ODS

**Tujuan:** Operational Data Store untuk real-time analytics dan reporting

**Komponen:**
- **PostgreSQL 16** - Database engine
- **Database:** `ods_db`
- **Schema:** `ods_schema.sql`

**Tables:**
- `customers` - Replikasi dari MySQL
- `credit_applications` - Replikasi dari MySQL
- `vehicle_ownership` - Replikasi dari MySQL

**Features:**
- **CDC Metadata:**
  - `cdc_timestamp` - Timestamp dari source
  - `cdc_operation` - Operation type (c/u/d)
- **Upsert Support:** ON CONFLICT DO UPDATE
- **Indexes:** Untuk performance query

**Lokasi:**
- Schema: `ods_schema.sql`
- Data: `ods_data/` (volume)

---

## Data Flow

### 1. Initial Snapshot (First Run)

```
MySQL → Debezium → Kafka → Custom Consumer → PostgreSQL
  ↓         ↓         ↓           ↓              ↓
Data    Capture   Publish    Consume      Insert
```

1. Debezium melakukan initial snapshot dari MySQL
2. Semua existing records di-publish ke Kafka
3. Custom consumer consume dan insert ke PostgreSQL

### 2. Real-time CDC (Ongoing)

```
MySQL Change → Binlog → Debezium → Kafka → Consumer → PostgreSQL
     ↓           ↓         ↓         ↓         ↓          ↓
  INSERT/     Capture   Convert   Publish   Consume   Upsert
  UPDATE/     Change    Format    Message   Message   Data
  DELETE
```

1. User melakukan INSERT/UPDATE/DELETE di MySQL
2. MySQL menulis ke binary log
3. Debezium membaca binlog dan capture change
4. Debezium convert dan publish ke Kafka topic
5. Custom consumer consume message
6. Consumer convert format dan upsert ke PostgreSQL

### 3. Message Format Conversion

**Debezium Format:**
```json
{
  "date_of_birth": 2909,  // epoch days
  "monthly_income": "SSdQ+A==",  // base64 decimal
  "registration_date": 1766131347000  // milliseconds
}
```

**PostgreSQL Format:**
```sql
date_of_birth: '1977-12-19'
monthly_income: 10000000.00
registration_date: '2025-12-19 08:02:27'
```

---

## Teknologi Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| Source | MySQL | 8.0 | Source database |
| CDC | Debezium | 2.4 | Change data capture |
| Message Broker | Apache Kafka | 7.5.0 | Streaming platform |
| Coordination | Zookeeper | 7.5.0 | Kafka coordination |
| Sink | Python | 3.9+ | Custom consumer |
| Target | PostgreSQL | 16 | ODS database |
| Orchestration | Docker Compose | - | Container orchestration |

---

## Karakteristik Arsitektur

### 1. **Decoupled Architecture**
- Source (MySQL) dan Target (PostgreSQL) terpisah
- Kafka sebagai buffer/message broker
- Tidak ada direct coupling

### 2. **Real-time Processing**
- Near real-time replication (latency < 1 detik)
- Event-driven architecture
- No batch processing

### 3. **Scalability**
- Kafka dapat scale horizontal
- Multiple consumers dapat run parallel
- Partition dapat di-scale

### 4. **Fault Tolerance**
- Kafka persistence (messages tidak hilang)
- Consumer dapat restart dan resume dari offset
- PostgreSQL transaction support

### 5. **Data Consistency**
- Upsert dengan ON CONFLICT untuk idempotency
- CDC metadata untuk tracking
- No data loss (Kafka retention)

---

## Deployment Architecture

```
┌─────────────────────────────────────────────────────────┐
│              Docker Compose Network                     │
│                  (data-net)                            │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐            │
│  │  MySQL   │  │  Kafka   │  │ Postgres │            │
│  │          │  │          │  │          │            │
│  │ :3306    │  │ :9092    │  │ :5432    │            │
│  └──────────┘  └──────────┘  └──────────┘            │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐            │
│  │Zookeeper │  │Kafka     │  │Kafka UI  │            │
│  │          │  │Connect   │  │          │            │
│  │ :2181    │  │ :8083    │  │ :8080    │            │
│  └──────────┘  └──────────┘  └──────────┘            │
│                                                         │
└─────────────────────────────────────────────────────────┘
                          │
                          │
                    ┌─────┴─────┐
                    │   Host    │
                    │           │
                    │ Python    │
                    │ Consumer  │
                    └───────────┘
```

**Network:**
- Internal: `data-net` (Docker network)
- Services communicate via service names
- Ports exposed untuk external access

**Volumes:**
- `mysql_data/` - MySQL data persistence
- `ods_data/` - PostgreSQL data persistence
- `schema_history/` - Debezium schema history

---

## Monitoring & Observability

### 1. **Kafka UI** (http://localhost:8080)
- View topics and messages
- Monitor consumer groups
- Check connector status

### 2. **PostgreSQL Logs**
```bash
docker logs postgres --tail 100 --follow
```

### 3. **Kafka Connect Logs**
```bash
docker logs kafka-connect --tail 100 --follow
```

### 4. **Scripts**
- `verify_ods.py` - Verify data in ODS
- `check_connector_status.py` - Check CDC status
- `check_kafka_topics.py` - Check Kafka topics
- `query_ods.py` - Query ODS data

---

## Performance Considerations

### 1. **Throughput**
- Kafka: High throughput (100k+ messages/sec)
- Consumer: Batch processing (100 messages/batch)
- PostgreSQL: Batch inserts dengan transaction

### 2. **Latency**
- End-to-end: < 1 second (typical)
- Depends on:
  - MySQL binlog flush
  - Kafka producer
  - Consumer processing
  - PostgreSQL commit

### 3. **Resource Usage**
- Memory: ~2GB per service
- CPU: Low (event-driven)
- Disk: Depends on retention

---

## Security Considerations

### 1. **Network Isolation**
- Docker network (internal)
- Ports exposed only if needed

### 2. **Authentication**
- MySQL: User/password
- PostgreSQL: User/password
- Kafka: No auth (development)

### 3. **Data Encryption**
- Not implemented (development)
- Production: TLS/SSL recommended

---

## Future Improvements

### 1. **Schema Registry**
- Use Confluent Schema Registry
- Better schema evolution
- Compatibility checking

### 2. **Multiple Consumers**
- Different consumer groups
- Different processing logic
- Fan-out pattern

### 3. **Error Handling**
- Dead Letter Queue (DLQ)
- Retry mechanism
- Alerting

### 4. **Monitoring**
- Prometheus metrics
- Grafana dashboards
- Alert manager

### 5. **High Availability**
- Kafka cluster (multiple brokers)
- PostgreSQL replication
- Consumer failover

---

## File Structure

```
mini-poc-stack/
├── docker-compose.yml              # Service definitions
├── ods_schema.sql                  # PostgreSQL schema
├── requirements.txt                # Python dependencies
├── README_SETUP.md                 # Setup guide
├── README_ODS_ACCESS.md            # ODS access guide
├── ARCHITECTURE.md                 # This file
│
├── debezium-connector-config/
│   └── mysql-connector.json        # CDC connector config
│
└── py_script/
    ├── setup_cdc.py                # Setup CDC connector
    ├── custom_ods_sink.py          # Custom consumer
    ├── setup_full_pipeline.py      # Full pipeline setup
    ├── reset_all.py                # Reset/cleanup
    ├── verify_ods.py               # Verify ODS data
    ├── check_connector_status.py    # Check CDC status
    ├── check_kafka_topics.py       # Check Kafka topics
    └── query_ods.py                # Query ODS helper
```

---

## Summary

Arsitektur ini mengimplementasikan **CDC pattern** dengan:
- **Source:** MySQL (transactional database)
- **CDC:** Debezium (change capture)
- **Broker:** Kafka (message streaming)
- **Sink:** Custom Python Consumer (format conversion)
- **Target:** PostgreSQL ODS (operational store)

**Key Benefits:**
- Real-time data replication
- Decoupled architecture
- Scalable and fault-tolerant
- Easy to monitor and maintain

