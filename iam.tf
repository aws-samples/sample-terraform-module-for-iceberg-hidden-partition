# Data sources for constructing ARNs
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# IAM Role for Glue Service
resource "aws_iam_role" "glue_service_role" {
  name = "${local.required_tags.ProductName}-${local.required_tags.CapabilityName}-glue-service-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
      {
        Effect = "Allow"
        Principal = {
          AWS = data.aws_caller_identity.current.arn
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = "terraform-iceberg-partition"
          }
        }
      }
    ]
  })

  tags = local.default_additional_tags
}

# IAM Policy for Glue to access S3
resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*"
        ]
      }
    ]
  })
}

# IAM Policy for Glue Catalog operations
resource "aws_iam_role_policy" "glue_catalog_policy" {
  name = "glue-catalog-access"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Glue Database and Table operations - scoped to account resources
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:UpdateDatabase",
          "glue:DeleteDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchGetPartition"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*"
        ]
      },
      # Glue Job operations - scoped to account resources
      {
        Effect = "Allow"
        Action = [
          "glue:GetJob",
          "glue:GetJobs",
          "glue:CreateJob",
          "glue:UpdateJob",
          "glue:DeleteJob",
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/*"
        ]
      },
      # Athena operations - scoped to region and account
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup"
        ]
        Resource = [
          "arn:aws:athena:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:workgroup/*"
        ]
      },
      # S3 access for Athena query results
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
          "s3:PutObject"
        ]
        Resource = [
          "arn:aws:s3:::aws-athena-query-results-*",
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*"
        ]
      }
    ]
  })
}

# IAM Policy for CloudWatch Logs
resource "aws_iam_role_policy" "glue_cloudwatch_policy" {
  name = "glue-cloudwatch-logs"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*"
        ]
      }
    ]
  })
}

# IAM Policy for PassRole (needed for creating Glue jobs)
resource "aws_iam_role_policy" "glue_passrole_policy" {
  name = "glue-passrole"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = aws_iam_role.glue_service_role.arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "glue.amazonaws.com"
          }
        }
      }
    ]
  })
}
