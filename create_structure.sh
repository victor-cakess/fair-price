#!/bin/bash
# Fair-Price Project Structure Creator
# Creates the complete directory structure for the project

echo "🏗️  Creating Fair-Price project structure..."

# Create directories
echo "📁 Creating directories..."
mkdir -p src
mkdir -p notebooks
mkdir -p data/raw
mkdir -p data/processed
mkdir -p data/reports
mkdir -p docs
mkdir -p tests
mkdir -p infrastructure/terraform
mkdir -p scripts
mkdir -p .github/workflows

# Create empty files
echo "📄 Creating files..."

# Core modules
touch src/__init__.py
touch src/extraction.py
touch src/exploration.py
touch src/standardization.py
touch src/utils.py
touch src/config.py

# Interactive showcase
touch notebooks/data-pipeline-showcase.ipynb

# Documentation
touch docs/exploration-framework.md
touch docs/standardization-framework.md
touch docs/extraction-strategy.md
touch docs/architecture-overview.md

# Tests
touch tests/__init__.py
touch tests/test_extraction.py
touch tests/test_exploration.py
touch tests/test_standardization.py
touch tests/test_utils.py

# Infrastructure
touch infrastructure/Dockerfile
touch infrastructure/docker-compose.yml
touch infrastructure/cloudbuild.yaml
touch infrastructure/terraform/main.tf
touch infrastructure/terraform/variables.tf
touch infrastructure/terraform/outputs.tf

# Scripts
touch scripts/run_pipeline.py
touch scripts/setup.sh
touch scripts/deploy.sh

# CI/CD
touch .github/workflows/ci.yml
touch .github/workflows/cd.yml

# Root files
touch Makefile
touch requirements.txt
touch requirements-dev.txt
touch .env.example

touch .dockerignore


# Make scripts executable
chmod +x scripts/setup.sh
chmod +x scripts/deploy.sh

echo "✅ Fair-Price project structure created successfully!"
echo "📁 Project location: $(pwd)"
echo ""
echo "📋 Structure created:"
echo "fair-price/"
echo "├── src/"
echo "├── notebooks/"
echo "├── data/"
echo "│   ├── raw/"
echo "│   ├── processed/"
echo "│   └── reports/"
echo "├── docs/"
echo "├── tests/"
echo "├── infrastructure/"
echo "│   └── terraform/"
echo "├── scripts/"
echo "├── .github/"
echo "│   └── workflows/"
echo "└── [config files]"
echo ""
echo "🚀 Ready for development!"