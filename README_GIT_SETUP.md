# Setup untuk Git Repository

Dokumentasi ini menjelaskan perubahan yang dilakukan untuk mempersiapkan repository untuk di-upload ke Git.

## Perubahan yang Dilakukan

### 1. **Credentials & Sensitive Data**

Semua password dan credentials telah dipindahkan ke environment variables:

- **File `.env`** - Berisi semua credentials (tidak di-commit ke Git)
- **File `.env.example`** - Template untuk setup (di-commit ke Git)
- **`.gitignore`** - Mengabaikan file `.env` dan data directories

### 2. **Nama yang Digeneralisasi**

Beberapa nama spesifik telah digeneralisasi:

- `mks_finance_dw` → `${MYSQL_DATABASE}` atau `${DEBEZIUM_DATABASE_NAME}`
- `mks-finance-mysql-connector` → `${DEBEZIUM_CONNECTOR_NAME}`
- `mks_finance` (topic prefix) → `${DEBEZIUM_TOPIC_PREFIX}`

### 3. **Data Directories**

Semua data directories di-ignore oleh Git:
- `mysql_data/`
- `ods_data/`
- `postgres_data/`
- `schema_history/`

## Setup Setelah Clone

### 1. Copy Environment File

```bash
cp .env.example .env
```

### 2. Edit `.env` File

Edit file `.env` dan sesuaikan dengan environment Anda:

```bash
# MySQL Configuration
MYSQL_ROOT_PASSWORD=your_secure_password
MYSQL_DATABASE=your_database_name
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_mysql_password

# PostgreSQL ODS Configuration
POSTGRES_USER=ods_user
POSTGRES_PASSWORD=your_secure_ods_password
POSTGRES_DB=ods_db

# Debezium Configuration
DEBEZIUM_CONNECTOR_NAME=your-connector-name
DEBEZIUM_TOPIC_PREFIX=your_topic_prefix
DEBEZIUM_DATABASE_NAME=your_database_name
```

### 3. Update Connector Config

Copy dan edit connector config:

```bash
cp debezium-connector-config/mysql-connector.json.example \
   debezium-connector-config/mysql-connector.json
```

Edit `mysql-connector.json` dan sesuaikan dengan environment variables atau hardcode values.

**Atau** gunakan script setup yang akan generate config dari environment variables.

### 4. Setup Database Schema

Pastikan file SQL schema sesuai dengan nama database Anda:
- `mks_finance_dw.sql` - Update nama database jika perlu
- `ods_schema.sql` - Sudah generic, tidak perlu diubah

### 5. Start Services

```bash
docker-compose up -d
```

## File yang Perlu Diperhatikan

### ✅ **File yang Aman untuk Git:**
- `docker-compose.yml` - Menggunakan environment variables
- `*.py` scripts - Menggunakan environment variables
- `*.sql` schema files
- `*.md` documentation
- `.env.example` - Template only

### ❌ **File yang TIDAK di-commit:**
- `.env` - Berisi credentials
- `mysql_data/` - Database data
- `ods_data/` - Database data
- `postgres_data/` - Database data
- `*.log` - Log files

## Customization Guide

### Mengganti Nama Database

1. Update `.env`:
   ```bash
   MYSQL_DATABASE=your_new_database_name
   DEBEZIUM_DATABASE_NAME=your_new_database_name
   ```

2. Update `mysql-connector.json`:
   ```json
   "database.include.list": "your_new_database_name",
   "table.include.list": "your_new_database_name.customers,..."
   ```

3. Update schema file name atau content jika perlu

### Mengganti Connector Name

1. Update `.env`:
   ```bash
   DEBEZIUM_CONNECTOR_NAME=your-connector-name
   ```

2. Update `mysql-connector.json`:
   ```json
   "name": "your-connector-name"
   ```

### Mengganti Topic Prefix

1. Update `.env`:
   ```bash
   DEBEZIUM_TOPIC_PREFIX=your_topic_prefix
   ```

2. Update `mysql-connector.json`:
   ```json
   "topic.prefix": "your_topic_prefix"
   ```

3. Update Python scripts yang reference topics:
   - `custom_ods_sink.py` - Update `TOPICS` list
   - `check_kafka_topics.py` - Update topic names

## Security Best Practices

1. **Never commit `.env` file** - Sudah di-ignore oleh `.gitignore`
2. **Use strong passwords** - Generate secure passwords untuk production
3. **Rotate credentials** - Regularly update passwords
4. **Limit access** - Restrict who can access the repository
5. **Use secrets management** - For production, consider using:
   - AWS Secrets Manager
   - HashiCorp Vault
   - Kubernetes Secrets

## Troubleshooting

### Environment variables tidak terbaca

Pastikan:
1. File `.env` ada di root directory
2. Docker Compose membaca `.env` (default behavior)
3. Python scripts menggunakan `os.environ.get()` dengan default values

### Connector config error

Pastikan:
1. `mysql-connector.json` sudah di-copy dari `.example`
2. Values di config sesuai dengan `.env`
3. Database name dan table names sesuai

## Next Steps

Setelah setup:
1. Run `python py_script/setup_full_pipeline.py`
2. Verify dengan `python py_script/verify_ods.py`
3. Check documentation di `README_SETUP.md`

