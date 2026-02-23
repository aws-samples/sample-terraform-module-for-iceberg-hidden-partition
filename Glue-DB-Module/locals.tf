locals {
  # ----------------------------------------------------------------------------
  # Tags
  # ----------------------------------------------------------------------------
  internal_tags = {
    Creation-Time          = time_static.main["enabled"].rfc3339
    DeploymentTool         = "terraform"
    TerraformModule        = "terraform-aws-crm-glue-database"
    TerraformModuleVersion = "0.7.6" # x-release-please-version
  }

  tags = merge(
    var.additional_tags,
    var.required_tags,
    local.internal_tags,
  )

  # ----------------------------------------------------------------------------
  # Naming
  # ----------------------------------------------------------------------------

  name_base = var.iac_overrides.explicit_db_name ? join("_",
    compact([
      var.iac_overrides.prefix_explicit_name ? var.required_tags.ProductName : "",
      var.iac_overrides.prefix_explicit_name ? var.required_tags.CapabilityName : "",
      var.name_suffix
      ])) : replace(join("_",
      compact([
        var.required_tags.ProductName,
        var.required_tags.CapabilityName,
        data.aws_region.current.name,
        var.required_tags.Environment,
        var.name_suffix,
        random_id.main["enabled"].hex,
        "gluedb",
      ])
  ), "-", "_")

  # ----------------------------------------------------------------------------
  # Iceberg tables hidden Partitioning support
  # ----------------------------------------------------------------------------

  iceberg_schema_change = {
    for k, v in var.tables : k => v
    if var.iceberg_schema_config.athena_s3_output != "" && contains(["iceberg", "ICEBERG"], lookup(v.parameters, "table_type", ""))
  }

  partitioned_iceberg_tables = {
    for k, v in var.tables : k => v
    if contains(["iceberg", "ICEBERG"], lookup(v.parameters, "table_type", "")) && v.partition_transforms != null && var.iceberg_schema_config.athena_s3_output != ""
  }

  # Temp Glue job name for Iceberg partitioning
  temp_glue_job_name = replace("iceberg-partition-${local.name_base}", "_", "-")
}