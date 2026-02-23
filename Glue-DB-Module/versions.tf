terraform {
  required_version = ">= 1.3, < 2.0"

  required_providers {
    aws = {
      version = "< 6, > 5"
      source  = "hashicorp/aws"
    }
    random = {
      version = "< 4, >= 1.2"
      source  = "hashicorp/random"
    }
    time = {
      version = "< 1"
      source  = "hashicorp/time"
    }
  }
}
