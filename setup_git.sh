#!/bin/bash
# Script untuk setup Git repository dan push ke GitHub

echo "=========================================="
echo "Setup Git Repository"
echo "=========================================="

# 1. Initialize Git repository
echo ""
echo "1. Initializing Git repository..."
git init

# 2. Add all files
echo ""
echo "2. Adding files to Git..."
git add .

# 3. Create initial commit
echo ""
echo "3. Creating initial commit..."
git commit -m "Initial commit: Real-time CDC pipeline from MySQL to PostgreSQL ODS

- CDC implementation using Debezium
- Kafka message broker
- Custom Python consumer for ODS sink
- Complete documentation and setup scripts"

# 4. Add remote
echo ""
echo "4. Adding remote repository..."
git remote add origin https://github.com/hrizriz/real-time-cdc-pipeline.git

# 5. Check remote
echo ""
echo "5. Verifying remote..."
git remote -v

echo ""
echo "=========================================="
echo "✓ Git repository setup complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Review changes: git status"
echo "2. Push to GitHub: git push -u origin main"
echo ""
echo "If 'main' branch doesn't exist, use:"
echo "  git branch -M main"
echo "  git push -u origin main"

