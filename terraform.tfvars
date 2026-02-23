# AWS Configuration
aws_region = "us-east-2"

# IAM Role for Terraform Provider (not needed if using default credentials)
# provider_role_arn = "arn:aws:iam::205930618750:role/glue-test-role"

# Environment
environment = "dev"

# S3 Bucket Configuration
s3_bucket_name     = "demo-test-db-data-dev"
artifact_s3_bucket = "demo-test-db-data-dev"

# Application Version
app_version = "1.0.0"

# Build Directories (optional - using defaults)
# lambda_build_dir = "../../lambda_build"
# glue_build_dir   = "../../glue_build"
