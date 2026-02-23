terraform {
  required_version = ">=1.0, <2.0"
  # backend "s3" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "< 7, > 3"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "< 5"
    }
  }
}

provider "aws" {
  region = var.aws_region
}