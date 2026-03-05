# AWS Configuration
aws_region = "us-east-1" # Change to your target region
# Environment
environment = "production" # Options: dev, staging, production
# S3 Bucket Configuration
s3_bucket_name   = "your-iceberg-bucket-name" # For Iceberg tables data and metadata
athena_s3_bucket = "your-athena-bucket-name"  # For Athena query results
# Application Version
app_version = "1.0.0" # Update as needed
