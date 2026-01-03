# Setup Git Repository

Panduan untuk setup Git repository dan push ke GitHub.

## Langkah-langkah

### 1. Initialize Git Repository

```bash
cd real-time-cdc-pipeline
git init
```

### 2. Add Files

```bash
# Check files yang akan di-add
git status

# Add semua file (kecuali yang di .gitignore)
git add .
```

### 3. Create Initial Commit

```bash
git commit -m "Initial commit: Real-time CDC pipeline from MySQL to PostgreSQL ODS

- CDC implementation using Debezium
- Kafka message broker  
- Custom Python consumer for ODS sink
- Complete documentation and setup scripts"
```

### 4. Add Remote Repository

```bash
git remote add origin https://github.com/hrizriz/real-time-cdc-pipeline.git
```

### 5. Set Branch (jika perlu)

```bash
# Jika branch belum 'main', rename
git branch -M main
```

### 6. Push to GitHub

```bash
# Push ke GitHub
git push -u origin main
```

## Atau Gunakan Script Otomatis

```bash
# Make script executable (Linux/Mac)
chmod +x setup_git.sh

# Run script
./setup_git.sh
```

Atau di Windows (Git Bash):
```bash
bash setup_git.sh
```

## Verifikasi

```bash
# Check remote
git remote -v

# Check status
git status

# Check commits
git log --oneline
```

## Troubleshooting

### Error: "fatal: not a git repository"
- Pastikan sudah di folder `real-time-cdc-pipeline`
- Jalankan `git init` dulu

### Error: "remote origin already exists"
```bash
# Remove existing remote
git remote remove origin

# Add again
git remote add origin https://github.com/hrizriz/real-time-cdc-pipeline.git
```

### Error: "branch 'main' does not exist"
```bash
# Create and switch to main
git branch -M main
```

### Error: "authentication failed"
- Pastikan sudah login ke GitHub
- Atau gunakan Personal Access Token:
  ```bash
  git remote set-url origin https://[token]@github.com/hrizriz/real-time-cdc-pipeline.git
  ```

## File yang TIDAK akan di-commit

Berdasarkan `.gitignore`, file berikut tidak akan di-commit:
- `.env` (credentials)
- `mysql_data/`, `ods_data/`, `postgres_data/` (data directories)
- `*.log` files
- `__pycache__/`
- dll (lihat `.gitignore`)

## Checklist Sebelum Push

- [ ] File `.env` sudah di-ignore (cek dengan `git status`)
- [ ] File `mysql-connector.json` sudah di-ignore atau dihapus (gunakan `.example`)
- [ ] Data directories tidak muncul di `git status`
- [ ] Semua file penting sudah di-add
- [ ] Commit message sudah jelas

## Setelah Push

1. **Verify di GitHub:**
   - Buka https://github.com/hrizriz/real-time-cdc-pipeline
   - Pastikan semua file sudah ter-upload
   - Pastikan `.env` dan data directories TIDAK ada

2. **Add README badge** (optional):
   ```markdown
   ![GitHub](https://img.shields.io/github/license/hrizriz/real-time-cdc-pipeline)
   ```

3. **Add topics/tags** di GitHub:
   - `cdc`
   - `debezium`
   - `kafka`
   - `postgresql`
   - `mysql`
   - `data-pipeline`
   - `real-time`

