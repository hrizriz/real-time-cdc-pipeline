# Cara Akses dan Cek Data ODS

Ada beberapa cara untuk melihat data ODS (Operational Data Store) PostgreSQL:

## 1. Via psql (Command Line) - Di Container

```bash
# Akses PostgreSQL container
docker exec -it postgres psql -U ods_user -d ods_db

# Setelah masuk, jalankan query:
\dt                    # List semua tables
\d customers           # Deskripsi table customers
SELECT COUNT(*) FROM customers;
SELECT * FROM customers LIMIT 10;
SELECT * FROM credit_applications LIMIT 10;
SELECT * FROM vehicle_ownership LIMIT 10;

# Exit
\q
```

## 2. Via psql (Command Line) - Dari Host

```bash
# Jika PostgreSQL port exposed (cek docker-compose.yml)
psql -h localhost -p 5432 -U ods_user -d ods_db

# Password: ods_pwd
```

## 3. Via GUI Tools

### pgAdmin
- Download: https://www.pgadmin.org/
- Connection:
  - Host: localhost (atau IP VM)
  - Port: 5432 (jika exposed)
  - Database: ods_db
  - Username: ods_user
  - Password: ods_pwd

### DBeaver
- Download: https://dbeaver.io/
- Connection:
  - Type: PostgreSQL
  - Host: localhost (atau IP VM)
  - Port: 5432 (jika exposed)
  - Database: ods_db
  - Username: ods_user
  - Password: ods_pwd

### VS Code Extension
- Install extension: "PostgreSQL" atau "SQLTools"
- Add connection dengan config di atas

## 4. Via Kafka UI (Cek Messages di Kafka)

Kafka UI sudah tersedia di:
- URL: http://localhost:8080 (atau IP VM:8080)
- Buka browser dan akses URL tersebut
- Cari topics:
  - `mks_finance.mks_finance_dw.customers`
  - `mks_finance.mks_finance_dw.credit_applications`
  - `mks_finance.mks_finance_dw.vehicle_ownership`
- Klik topic untuk melihat messages

## 5. Via Docker Logs

```bash
# Cek log PostgreSQL
docker logs postgres --tail 100

# Cek log dengan filter
docker logs postgres --tail 100 | grep -i "statement\|execute\|error"

# Follow log real-time
docker logs postgres --follow
```

## 6. Via Python Script (yang sudah ada)

```bash
# Verify data
python py_script/verify_ods.py

# Check connector status
python py_script/check_connector_status.py

# Check Kafka topics
python py_script/check_kafka_topics.py
```

## 7. Quick Query Commands

```bash
# Count records
docker exec postgres psql -U ods_user -d ods_db -c "SELECT COUNT(*) FROM customers;"
docker exec postgres psql -U ods_user -d ods_db -c "SELECT COUNT(*) FROM credit_applications;"
docker exec postgres psql -U ods_user -d ods_db -c "SELECT COUNT(*) FROM vehicle_ownership;"

# Sample data
docker exec postgres psql -U ods_user -d ods_db -c "SELECT * FROM customers LIMIT 5;"

# Check latest records
docker exec postgres psql -U ods_user -d ods_db -c "SELECT customer_id, full_name, last_updated FROM customers ORDER BY last_updated DESC LIMIT 10;"
```

## 8. Port Exposure (Jika perlu akses dari luar)

Jika ingin akses dari host machine atau GUI tools, pastikan port PostgreSQL exposed di `docker-compose.yml`:

```yaml
postgres:
  ports:
    - "5432:5432"  # Expose port 5432
```

**Note:** Defaultnya port mungkin tidak exposed untuk security. Jika perlu, tambahkan di docker-compose.yml.

## 9. Query Berguna

```sql
-- Cek total records per table
SELECT 
    'customers' as table_name, COUNT(*) as count FROM customers
UNION ALL
SELECT 
    'credit_applications', COUNT(*) FROM credit_applications
UNION ALL
SELECT 
    'vehicle_ownership', COUNT(*) FROM vehicle_ownership;

-- Cek latest updates
SELECT 
    customer_id, 
    full_name, 
    last_updated,
    cdc_operation,
    cdc_timestamp
FROM customers 
ORDER BY cdc_timestamp DESC 
LIMIT 10;

-- Cek data per hari
SELECT 
    DATE(cdc_timestamp) as date,
    COUNT(*) as count
FROM customers
GROUP BY DATE(cdc_timestamp)
ORDER BY date DESC;
```

