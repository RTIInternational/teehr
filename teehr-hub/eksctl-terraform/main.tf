terraform {
  required_providers {
    aws = {
      # ref: https://registry.terraform.io/providers/hashicorp/aws/latest
      source  = "hashicorp/aws"
      version = "~> 4.52"
    }

  }
  backend "s3" {
    bucket = "teehr-terraform-state"
    key    = "terraform/eksctl-state"
    region = "us-east-2"
  }
}

provider "random" {}

provider "aws" {
  region = var.region
}

data "aws_eks_cluster" "cluster" {
  name = var.cluster_name
}
