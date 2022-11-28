variable "project" {
  type        = string
  default     = "kf-task-fhir-etl"
  description = "A project namespace for the infrastructure."
}

variable "repo_name" {
  type        = string
  description = "Repository for the project in <org>/<repo> format"
}

locals {
  # e.g., "Some ETL Name" → "SOmeEtlName" it will print the first 10 characters 
  short = format("%s-%s-",substr(replace(title(lower(var.project)), " ", ""),0,11),var.environment)
}

variable "environment" {
  type        = string
  description = "An environment namespace for the infrastructure."
}

variable "region" {
  type        = string
  default     = "us-east-1"
  description = "A valid AWS region to configure the underlying AWS SDK."
}

variable "vpc_id" {
  type = string
}

variable "vpc_availability_zones" {
  type = list(string)
}

variable "vpc_private_subnet_ids" {
  type = list(string)
}

variable "batch_root_block_device_size" {
  type    = number
  default = 32
}

variable "batch_root_block_device_type" {
  type    = string
  default = "gp3"
}

variable "batch_spot_fleet_allocation_strategy" {
  type    = string
  default = "SPOT_CAPACITY_OPTIMIZED"
}

variable "batch_spot_fleet_bid_percentage" {
  type    = number
  default = 64
}

variable "batch_min_vcpus" {
  type    = number
  default = 0
}

variable "batch_max_vcpus" {
  type    = number
  default = 256
}

variable "etl_vcpus" {
  type    = number
  default = 1
}

variable "etl_memory" {
  type    = number
  default = 4096
}

variable "batch_instance_types" {
  type    = list(string)
  default = ["c5d", "m5d", "z1d"]
}

variable "image_name" {
  type    = string
  default = "232196027141.dkr.ecr.us-east-1.amazonaws.com/kf-task-fhir-etl"
}

variable "image_tag" {
  type    = string
  default = "latest"
}


variable "fhir_username" {
  type      = string
  sensitive = true
}

variable "fhir_password" {
  type      = string
  sensitive = true
}

variable "fhir_service_url" {
  type      = string
  sensitive = true
}

variable "aws_batch_service_role_policy_arn" {
  type    = string
  default = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

variable "aws_spot_fleet_service_role_policy_arn" {
  type    = string
  default = "arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"
}

variable "aws_ec2_service_role_policy_arn" {
  type    = string
  default = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

variable "aws_administrator_access_policy_arn" {
  type    = string
  default = "arn:aws:iam::aws:policy/AdministratorAccess"
}

variable "refs_that_can_assume_github_actions_role" {
  type = list(string)
  default = [
    "refs/heads/develop",
    "refs/heads/feature/**",
    "refs/heads/main"
  ]
}
