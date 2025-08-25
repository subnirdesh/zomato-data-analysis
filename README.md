# 🍽️ Zomato Data ETL Pipeline - Complete User Manual

## 📖 Table of Contents
1. [What is this Project?](#what-is-this-project)
2. [What You'll Need (Prerequisites)](#what-youll-need-prerequisites)
3. [Step-by-Step Installation Guide](#step-by-step-installation-guide)
4. [Setting Up Your Data](#setting-up-your-data)
5. [Running the Pipeline](#running-the-pipeline)
6. [Troubleshooting Common Issues](#troubleshooting-common-issues)
7. [Project Structure Explained](#project-structure-explained)

---

## 🎯 What is this Project?

This project is a **data processing pipeline** for Zomato restaurant data. Think of it as a smart system that:

- **Extracts** data from CSV files (like restaurant info, food items, user orders)
- **Transforms** and cleans the data (removes duplicates, fixes formatting)
- **Loads** the processed data into a database for easy querying

**Real-world use case**: Restaurant chains, food delivery companies, or data analysts can use this to analyze customer behavior, restaurant performance, and food trends.

**What you get**: Clean, organized data tables that can answer questions like:
- Which restaurants are most popular in each city?
- What are customer spending patterns?
- Which cuisines perform best?
- How do user demographics affect ordering behavior?

---

## 🛠️ What You'll Need (Prerequisites)

### 1. Computer Requirements
- **Operating System**: Windows 10+, macOS 10.14+, or Ubuntu 18.04+
- **RAM**: At least 8GB (16GB recommended)
- **Storage**: At least 5GB free space
- **Internet Connection**: Required for downloading software

### 2. Software to Install
Don't worry! We'll guide you through installing everything step by step.

**Required Software:**
- Python 3.8 or higher
- Java 11 (specific version needed for compatibility)
- PostgreSQL database
- Git (to download the code)

---

## 🚀 Step-by-Step Installation Guide

### Step 1: Install Python

#### For Windows:
1. Go to [python.org](https://python.org/downloads/)
2. Click "Download Python 3.11.x" (latest stable version)
3. Run the installer
4. **IMPORTANT**: Check "Add Python to PATH" during installation
5. Click "Install Now"

#### For macOS:
```bash
# Install Homebrew first (if not already installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Python
brew install python@3.11
```

#### For Ubuntu/Linux:
```bash
sudo apt update
sudo apt install python3.11 python3.11-pip python3.11-venv
```

**Verify Installation:**
```bash
python --version
# Should show: Python 3.11.x
```

### Step 2: Install Java 11

#### For Windows:
1. Go to [Oracle JDK 11](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html) or [OpenJDK 11](https://jdk.java.net/11/)
2. Download "Windows x64 Installer"
3. Run the installer and follow instructions
4. Add Java to PATH:
   - Search "Environment Variables" in Windows
   - Add `C:\Program Files\Java\jdk-11.x.x\bin` to PATH

#### For macOS:
```bash
# Using Homebrew
brew install openjdk@11

# Add to your shell profile (.zshrc or .bash_profile)
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@11"' >> ~/.zshrc
source ~/.zshrc
```

#### For Ubuntu/Linux:
```bash
sudo apt update
sudo apt install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

**Verify Installation:**
```bash
java -version
# Should show: openjdk version "11.x.x"
```

### Step 3: Install PostgreSQL Database

#### For Windows:
1. Go to [PostgreSQL Downloads](https://www.postgresql.org/download/windows/)
2. Download PostgreSQL 14 or 15
3. Run installer
4. **Remember your password!** You'll need it later
5. Default port: 5432 (keep this)

#### For macOS:
```bash
# Using Homebrew
brew install postgresql@14
brew services start postgresql@14

# Create a database user
createuser --interactive --pwprompt
# Enter username: postgres
# Enter password: (choose a secure password)
```

#### For Ubuntu/Linux:
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Set password for postgres user
sudo -u postgres psql
\password postgres
# Enter your password twice
\q
```

**Verify Installation:**
```bash
psql --version
# Should show: psql (PostgreSQL) 14.x
```

### Step 4: Install Git

#### For Windows:
1. Go to [git-scm.com](https://git-scm.com/download/win)
2. Download and run the installer
3. Use default settings (just keep clicking "Next")

#### For macOS:
```bash
# Git comes pre-installed, but you can update it
brew install git
```

#### For Ubuntu/Linux:
```bash
sudo apt update
sudo apt install git
```

**Verify Installation:**
```bash
git --version
# Should show: git version 2.x.x
```

### Step 5: Download the Project Code

1. **Open Terminal/Command Prompt:**
   - Windows: Press `Win + R`, type `cmd`, press Enter
   - macOS: Press `Cmd + Space`, type "Terminal", press Enter
   - Linux: Press `Ctrl + Alt + T`

2. **Navigate to where you want the project:**
```bash
cd Desktop  # or wherever you want to save the project
```

3. **Download the code:**
```bash
git clone https://github.com/YOUR_USERNAME/zomato-etl-pipeline.git
cd zomato-etl-pipeline
```

### Step 6: Set Up Python Environment

```bash
# Create a virtual environment
python -m venv venv

# Activate it
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# You should see (venv) at the beginning of your command prompt

# Install required packages
pip install -r requirements.txt
```

**If requirements.txt doesn't exist, install packages manually:**
```bash
pip install pyspark==3.4.1
pip install psycopg2-binary
pip install pandas
```

---

## 📊 Setting Up Your Data

### Step 1: Prepare Your CSV Files

Create a folder called `data` in your project directory and place these CSV files:

#### Required Files:
1. **food.csv** - Contains food items
   ```
   f_id,item,veg_or_non_veg
   F001,Butter Chicken,Non-Veg
   F002,Dal Makhani,Veg
   ```

2. **restaurant.csv** - Contains restaurant information
   ```
   id,name,city,rating,rating_count,cost,cuisine,lic_no,link
   1,Spice Garden,Mumbai,4.2,150,800,Indian,LIC001,http://example.com
   ```

3. **menu.csv** - Links restaurants to food items with prices
   ```
   menu_id,r_id,f_id,cuisine,price
   M001,1,F001,Indian,350
   ```

4. **users.csv** - Contains user information
   ```
   user_id,name,email,password,Age,Gender,Martial Status,Occupation,Monthly Income
   1,John Doe,john@email.com,password123,25,Male,Single,Engineer,50000
   ```

5. **orders.csv** - Contains order transactions
   ```
   order_date,sales_qty,sales_amount,currency,user_id,r_id
   2024-01-15,2,700,INR,1,1
   ```

### Step 2: Create Output Directory
```bash
mkdir output
```

---

## 🏃 Running the Pipeline

The pipeline has three main stages that run in sequence:

### Stage 1: Extract and Clean Data
```bash
# Make sure you're in the project directory and virtual environment is active
cd /path/to/zomato-etl-pipeline
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Run the transformation pipeline
python transform/execute.py data/ output/
```

**What this does:**
- Reads your CSV files
- Cleans the data (removes duplicates, fixes formatting)
- Saves cleaned data in Parquet format for faster processing

### Stage 2: Load Data to Database

First, create a database:
```bash
# Connect to PostgreSQL
psql -U postgres -h localhost

# Create database
CREATE DATABASE zomato_db;
\q
```

Then load the data:
```bash
python load/execute.py output/ zomato_db localhost 5432 postgres YOUR_PASSWORD
```

**Replace `YOUR_PASSWORD` with the PostgreSQL password you set during installation.**

### Complete Pipeline (All at Once)
```bash
# Run everything in sequence
python transform/execute.py data/ output/
python load/execute.py output/ zomato_db localhost 5432 postgres YOUR_PASSWORD
```

---


---

## 🔧 Troubleshooting Common Issues

### Issue 1: "Java Error" or "DirectByteBuffer" Error
**Problem**: Spark can't start due to Java version compatibility

**Solution**:
```bash
# Check Java version
java -version

# If you have Java 17+, install Java 11
# Windows: Download from Oracle website
# macOS: 
brew install openjdk@11
export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
# Linux:
sudo apt install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### Issue 2: "ModuleNotFoundError: No module named 'pyspark'"
**Problem**: PySpark not installed or virtual environment not activated

**Solution**:
```bash
# Activate virtual environment
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install PySpark
pip install pyspark==3.4.1
```

### Issue 3: "Connection refused" to PostgreSQL
**Problem**: Database not running or wrong credentials

**Solution**:
```bash
# Start PostgreSQL service
# Windows: Check Services app
# macOS: brew services start postgresql@14
# Linux: sudo systemctl start postgresql

# Test connection
psql -U postgres -h localhost
```

### Issue 4: "File not found" for CSV files
**Problem**: CSV files not in the correct location

**Solution**:
```bash
# Check current directory
pwd
ls -la

# Make sure your CSV files are in a 'data' folder
mkdir data
# Copy your CSV files to the data folder
```

### Issue 5: "Permission denied" errors
**Problem**: File system permissions

**Solution**:
```bash
# Make scripts executable (macOS/Linux)
chmod +x transform/execute.py
chmod +x load/execute.py

# Or run with python explicitly
python transform/execute.py data/ output/
```

### Issue 6: Memory errors with large datasets
**Problem**: Not enough memory for processing

**Solution**:
Add memory configuration to the Spark session or process smaller chunks:
```bash
# Edit transform/execute.py and modify create_spark_session():
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "4g")
```

---

## 📁 Project Structure Explained

```
zomato-etl-pipeline/
├── README.md                 # This guide
├── requirements.txt          # Python packages needed
├── data/                    # Your CSV files go here
│   ├── food.csv
│   ├── restaurant.csv
│   ├── menu.csv
│   ├── users.csv
│   └── orders.csv
├── transform/               # Data processing code
│   └── execute.py          # Main transformation script
├── load/                   # Database loading code
│   └── execute.py         # Database loading script
├── utility/               # Helper functions
│   └── utility.py        # Logging and utility functions
├── output/               # Processed data files (created automatically)
│   ├── stage1/          # Cleaned raw data
│   ├── stage2/          # Master tables
│   └── stage3/          # Analytics tables
└── venv/                # Python virtual environment
```

**What each file does:**

- **transform/execute.py**: Reads CSV files, cleans data, creates master tables
- **load/execute.py**: Loads processed data into PostgreSQL database
- **utility/utility.py**: Contains logging and helper functions
- **requirements.txt**: Lists all Python packages needed

---



## 🎉 Congratulations!

You've successfully set up and run the Zomato ETL pipeline! Your data is now clean, organized, and ready for analysis.

