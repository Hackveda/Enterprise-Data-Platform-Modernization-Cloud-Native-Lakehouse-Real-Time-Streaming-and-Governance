terraform {
  required_version = ">= 1.6.0"
}

variable "environment" { type = string; default = "dev" }

# Reference IaC skeleton. Replace local PoV components with managed cloud modules:
# - object storage (S3/GCS)
# - managed Kafka/PubSub
# - Snowflake/BigQuery/Databricks
# - Kubernetes / managed compute
# - IAM, KMS, secrets, private networking and audit logging

output "target_state" {
  value = "${var.environment}: object-store + lakehouse + warehouse + streaming + governance"
}
