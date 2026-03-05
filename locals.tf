locals {
  required_tags = {
    # More information about these tags is available here,
    # https://mdtcrhf.visualstudio.com/CRHFMLifeInfrastructure/_wiki/wikis/MLife%20Product%20Documentation/1459/MLife-Resource-Tagging-and-Labeling
    CostCenter           = "CR61244.145"
    Environment          = var.environment
    ProductName          = "mlife"
    CapabilityName       = "dataanalytics"
    OwnerName            = "mlife-data-analytics"
    OwnerEmail           = "dl.altairhilltoppers@medtronic.com"
    DeploymentRepository = "mlife-dataanalytics-datalake-metadata-db"
  }

  default_additional_tags = merge(
    {
      # AWS Migration Acceleration Program Tag
      map-migrated = "d-server-02618do6av8xyb"
    },
    local.required_tags,
  )

  metadata_glue = "gluejob/glue_cdc_metadata"

  # create databases and tables from yaml files
  stg_vars = {
    s3_bucket_name = var.s3_bucket_name
  }


  athena_results_path     = "s3://${var.athena_s3_bucket}/athena-results/"
  partition_glue_job_name = "iceberg-partition-job"

  # Define the specific database you want to process
  glue_dbs = toset([
    for file in fileset("${path.module}/glue-catalog/*", "*.yaml") :
    basename(dirname(file))
  ])

  # get YAML files for each DB under glue-catalog
  glue_catalog_database = {
    for database in local.glue_dbs :

    database => {
      for table in fileset("${path.module}/glue-catalog/${database}", "*.yaml") :
      trimsuffix(table, ".yaml") => yamldecode(templatefile("${path.module}/glue-catalog/${database}/${table}", local.stg_vars))
    }
  }

}