# CDC to ODS Pipeline

Pipeline untuk Change Data Capture (CDC) dari MySQL ke Operational Data Store (ODS) PostgreSQL menggunakan Apache Kafka dan Debezium.

## 🏗️ Architecture

```
MySQL → Debezium → Kafka → Custom Consumer → PostgreSQL ODS
```

Lihat dokumentasi lengkap di [ARCHITECTURE.md](ARCHITECTURE.md)

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Copy environment template
cp env.example .env

# Edit .env file dengan credentials Anda
nano .env
```

### 2. Start Services

```bash
# Start semua services
docker-compose up -d

# Atau menggunakan script otomatis
python py_script/setup_full_pipeline.py
```

### 3. Verify

```bash
# Verify ODS data
python py_script/verify_ods.py

# Check CDC status
python py_script/check_connector_status.py

# Check Kafka topics
python py_script/check_kafka_topics.py
```

## 📚 Documentation

- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Arsitektur lengkap pipeline
- **[README_SETUP.md](README_SETUP.md)** - Setup guide step-by-step
- **[README_ODS_ACCESS.md](README_ODS_ACCESS.md)** - Cara akses data ODS
- **[README_GIT_SETUP.md](README_GIT_SETUP.md)** - Setup untuk Git repository

## 🔧 Configuration

### Environment Variables

Edit file `.env` (copy dari `env.example`):

```bash
# MySQL
MYSQL_ROOT_PASSWORD=your_password
MYSQL_DATABASE=your_database
MYSQL_USER=your_user
MYSQL_PASSWORD=your_password

# PostgreSQL ODS
POSTGRES_USER=ods_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=ods_db

# Debezium
DEBEZIUM_CONNECTOR_NAME=your-connector-name
DEBEZIUM_TOPIC_PREFIX=your_topic_prefix
DEBEZIUM_DATABASE_NAME=your_database_name
```

### Connector Config

Copy dan edit connector config:

```bash
cp debezium-connector-config/mysql-connector.json.example \
   debezium-connector-config/mysql-connector.json
```

Edit `mysql-connector.json` sesuai dengan environment Anda.

## 📁 Project Structure

```
mini-poc-stack/
├── docker-compose.yml              # Service definitions
├── ods_schema.sql                  # PostgreSQL schema
├── env.example                     # Environment template
├── requirements.txt                # Python dependencies
│
├── debezium-connector-config/
│   ├── mysql-connector.json        # CDC connector config (create from .example)
│   └── mysql-connector.json.example
│
└── py_script/
    ├── setup_cdc.py                # Setup CDC connector
    ├── custom_ods_sink.py          # Custom consumer
    ├── setup_full_pipeline.py      # Full pipeline setup
    ├── reset_all.py                # Reset/cleanup
    ├── verify_ods.py               # Verify ODS data
    ├── check_connector_status.py   # Check CDC status
    ├── check_kafka_topics.py       # Check Kafka topics
    └── query_ods.py                # Query ODS helper
```

## 🔐 Security

- **Never commit `.env` file** - Sudah di-ignore oleh `.gitignore`
- **Use strong passwords** - Generate secure passwords untuk production
- **Rotate credentials** - Regularly update passwords

## 🛠️ Requirements

- Docker & Docker Compose
- Python 3.9+
- Dependencies: `pip install -r requirements.txt`

## 📝 License

[Your License Here]

## 👥 Contributors

[Your Name/Team]

