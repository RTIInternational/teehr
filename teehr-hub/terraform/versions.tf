terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      # ref: https://registry.terraform.io/providers/hashicorp/aws/latest
      source  = "hashicorp/aws"
      version = "~> 4.52"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.10"
    }
  }

  backend "s3" {
    bucket = "teehr-terraform-state"
    key    = "terraform/state"
    region = "us-east-2"
  }
}
