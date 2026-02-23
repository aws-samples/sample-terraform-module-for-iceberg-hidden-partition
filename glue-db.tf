# Loop through each database in glue-catalog and create a Glue Catalog Database and its tables
module "glue_catalog_databases" {
  for_each        = local.glue_catalog_database
  source          = "./Glue-DB-Module"
  name_suffix     = each.key
  required_tags   = local.required_tags
  additional_tags = local.default_additional_tags

  location_uri = "s3://${var.s3_bucket_name}/${each.key}/"

  iac_overrides = {
    explicit_db_name     = true
    prefix_explicit_name = false
  }

  deploy_role_arn = aws_iam_role.glue_service_role.arn

  # Enable Iceberg features (schema management + partition management)
  iceberg_schema_config = {
    athena_s3_output = local.athena_results_path
    partition_glue_job_name = local.partition_glue_job_name
  }

  tables = each.value
}