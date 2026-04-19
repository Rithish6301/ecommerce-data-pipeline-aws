# 🚀 E-Commerce Data Pipeline (AWS)

## 📌 Overview
Built an end-to-end event-driven data pipeline using AWS services to process semi-structured e-commerce data.

## 🧱 Architecture
S3 → Lambda → AWS Glue → S3 (Processed Layer)

## ⚙️ Tech Stack
- AWS S3
- AWS Lambda
- AWS Glue (PySpark)
- Python (boto3)

## 🔄 Workflow
1. Data generated using Python script
2. Uploaded to S3 (raw layer)
3. Lambda triggered on file upload
4. Glue ETL job processes data
5. Clean data stored in S3 (processed layer)

## 🔍 Key Features
- Schema drift handling
- Null value handling
- Mixed data type resolution
- Incremental processing using Glue bookmarks

## 📊 Output
- JSON → Parquet conversion
- Optimized for analytics


## 🚀 Future Improvements
- Partitioning
- Data quality checks
- Dashboard layer
