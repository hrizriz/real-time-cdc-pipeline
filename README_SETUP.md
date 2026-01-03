# Setup Pipeline - CDC to ODS

Dokumentasi step-by-step untuk setup pipeline dari awal.

## Quick Start

### 1. Reset All (Optional - jika ingin mulai dari awal)

```bash
python py_script/reset_all.py
```

Ini akan:
- Delete semua Kafka Connect connectors
- Stop Docker Compose services
- (Optional) Remove volumes (hapus semua data)

### 2. Setup Full Pipeline

```bash
python py_script/setup_full_pipeline.py
```

Script ini akan otomatis:
1. Start Docker Compose services
2. Wait for services ready
3. Setup Debezium MySQL CDC Connector
4. Verify CDC is working
5. Run Custom ODS Sink Consumer
6. Verify ODS data

## Manual Step-by-Step

Jika ingin setup manual step-by-step:

### Step 1: Start Docker Compose

```bash
docker-compose up -d
```

Tunggu semua services ready (biasanya 30-60 detik).

### Step 2: Verify Services

```bash
# Check MySQL
docker exec mysql mysqladmin ping -h localhost --silent

# Check Kafka
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092

# Check Kafka Connect
curl http://localhost:8083/connectors

# Check PostgreSQL
docker exec postgres pg_isready -U ods_user
```

### Step 3: Setup CDC Connector

```bash
python py_script/setup_cdc.py
```

Ini akan membuat Debezium MySQL Connector yang akan:
- Capture changes dari MySQL
- Publish ke Kafka topics

### Step 4: Verify CDC

```bash
# Check connector status
python py_script/check_connector_status.py

# Check Kafka topics
python py_script/check_kafka_topics.py
```

### Step 5: Setup ODS Sink

**PENTING:** Pastikan sink connectors sudah di-delete (jika ada):

```bash
curl -X DELETE http://localhost:8083/connectors/jdbc-sink-customers
curl -X DELETE http://localhost:8083/connectors/jdbc-sink-credit-applications
curl -X DELETE http://localhost:8083/connectors/jdbc-sink-vehicle-ownership
```

Jalankan Custom ODS Sink Consumer:

```bash
python py_script/custom_ods_sink.py
```

Consumer ini akan:
- Read messages dari Kafka
- Convert format Debezium ke PostgreSQL
- Insert ke ODS PostgreSQL

Tekan `Ctrl+C` untuk stop consumer setelah semua data ter-process.

### Step 6: Verify ODS

```bash
python py_script/verify_ods.py
```

## Troubleshooting

### Services tidak ready

```bash
# Check logs
docker logs mysql
docker logs kafka
docker logs kafka-connect
docker logs postgres

# Restart service
docker-compose restart <service-name>
```

### CDC Connector tidak jalan

```bash
# Check connector status
python py_script/check_connector_status.py

# Check connector errors
docker logs kafka-connect --tail 100 | grep -i error

# Restart connector
curl -X POST http://localhost:8083/connectors/mks-finance-mysql-connector/restart
```

### ODS tidak ada data

```bash
# Check apakah ada message di Kafka
python py_script/check_kafka_topics.py

# Check consumer offset
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --group custom-ods-sink-* --describe

# Run consumer lagi (akan read dari awal karena group baru)
python py_script/custom_ods_sink.py
```

### Reset consumer offset (jika perlu)

```bash
# Delete consumer group
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --group custom-ods-sink-* --delete

# Atau reset offset
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --group custom-ods-sink-* --reset-offsets --to-earliest --topic mks_finance.mks_finance_dw.customers --execute
```

## Architecture

```
MySQL (Source)
    ↓ (Debezium CDC)
Kafka Topics
    ↓ (Custom Consumer)
PostgreSQL ODS (Sink)
```

## Files

- `docker-compose.yml` - Services definition
- `py_script/setup_cdc.py` - Setup CDC connector
- `py_script/custom_ods_sink.py` - Custom ODS sink consumer
- `py_script/verify_ods.py` - Verify ODS data
- `py_script/check_connector_status.py` - Check CDC status
- `py_script/check_kafka_topics.py` - Check Kafka topics

## Notes

- Custom ODS Sink Consumer menggunakan consumer group baru setiap run (dengan timestamp)
- Ini memastikan consumer selalu read dari awal
- Untuk production, pertimbangkan menggunakan persistent consumer group

