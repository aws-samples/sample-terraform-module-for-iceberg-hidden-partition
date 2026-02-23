variable "aws_region" {
  type        = string
  description = "AWS Region for configuring the Terraform Provider."
  default     = "us-east-1"
}

variable "provider_role_arn" {
  description = "The role to assume within the AWS provider"
  type        = string
  default     = null
}

variable "environment" {
  type        = string
  description = "environment name"

  validation {
    condition     = var.environment == lower(var.environment)
    error_message = "Environment string must be lower case."
  }
}

variable "artifact_s3_bucket" {
  type        = string
  description = "S3 bucket for storing artifacts"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for CDC data"
  type        = string
}

variable "app_version" {
  type        = string
  description = "Version identifier for Lambda and Glue artifacts (can be version or commit hash)"
}

variable "lambda_build_dir" {
  type        = string
  description = "Relative local directory containing Lambda function build artifacts"
  default     = "../../lambda_build"
}

variable "glue_build_dir" {
  type        = string
  description = "Relative local directory containing Glue job build artifacts"
  default     = "../../glue_build"
}